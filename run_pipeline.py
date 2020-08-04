########################################################################
#
# Run the ontology mapping pipeline on a set of key-value pairs 
# that describe a biological sample
#
########################################################################

from optparse import OptionParser
import json
import sys
from collections import defaultdict
import dill
import datetime
from multiprocessing import Pool
import pkg_resources as pr
from map_sra_to_ontology import config
from map_sra_to_ontology import pipeline_components as pc
from map_sra_to_ontology import run_sample_type_predictor
from os.path import join
import rdflib


def main():
    parser = OptionParser()
    parser.add_option("-f", "--key_value_file",
                      help="JSON file storing key-value pairs describing sample",
                      dest="input_filename")
    parser.add_option("-o", "--output", help="Output filename",
                      dest="output_filename", type="str", default="")
    parser.add_option("-i", "--init", help="init dill file",
                      dest="init_dill",
                      default=pr.resource_filename(__name__, "pipeline_init.dill"))
    parser.add_option("-k", "--keywords",
                      help="specified mapping keywords json",
                      dest="keywords_filename", type="str", default="")
    parser.add_option("-n", "--processes", help="# of processes",
                      dest="processes", type="int", default=1)
    parser.add_option("-d", "--debug", help="debug mode",
                      dest="dbg", action="store_true")
    parser.add_option("-t", "--test", help="test mode",
                      dest="tst", action="store_true")
    (options, args) = parser.parse_args()

    input_f    = options.input_filename
    output_f   = options.output_filename
    init_dill  = options.init_dill
    processes  = options.processes
    debug_mode = options.dbg
    keywords_f = options.keywords_filename
    test_mode  = options.tst

    # Map key-value pairs to ontologies
    with open(input_f, "r", encoding="utf-8") as f:
        biosample_json = json.load(f)

    tag_to_vals = []
    ct = datetime.datetime.now()
    sys.stderr.write('[{}] Parsing BioSample JSON\n'.format(ct))
    if test_mode:
        for tag_to_val in biosample_json:
            entry = {}
            entry["accession"] = tag_to_val["sample_accession"]
            for k in tag_to_val["attributes"]:
                val = tag_to_val["attributes"][k]
                if k != "accession" and len(val) < 100:
                    entry[k] = val
            tag_to_vals.append(entry)
    else:
        for tag_to_val in biosample_json:
            entry = {}
            entry["accession"] = tag_to_val["accession"]
            entry["taxId"] = str(tag_to_val.get("taxId", ""))
            for k in tag_to_val["characteristics"]:
                val = tag_to_val["characteristics"][k][0]["text"]
                if k != "accession" and len(val) < 100 and k != "taxId":
                    entry[k] = val
            tag_to_vals.append(entry)

    # Load ontologies
    ct = datetime.datetime.now()
    sys.stderr.write('[{}] Initializing pipeline.\n'.format(ct))
    # dill.load_session(init_dill)
    ont_name_to_ont_id = {
        "UBERON": "12",
        "CL": "1",
        "DOID": "2",
        "EFO": "16",
        "CVCL": "4",
        "ORDO": "19",
        "UBERON_all": "5",
        "UO": "7",
        "EFO_all": "9"
    }
    with open(init_dill, "rb") as f:
        vars = dill.load(f)
        pipeline = vars[0]
        ont_id_to_og = vars[1]

    # TWO_CHAR_MAPPINGS_JSON = "/mnt/c/Users/togotv_dell1/work/biosample/MetaSRA-pipeline/map_sra_to_ontology/metadata/two_char_mappings.json"
    if keywords_f != "":
        with open(keywords_f, "r") as f:
            pipeline.stages[14].str_to_mappings = json.load(f)

    all_mappings = []
    ct = datetime.datetime.now()
    sys.stderr.write('[{}] Mapping with {} processes.\n'.format(ct, processes))
    if processes == 1:
        i = 0
        covered_query_map = dict()
        for tag_to_val in tag_to_vals:
            if i % 2 == 0 and debug_mode:
                ct = datetime.datetime.now()
                sys.stderr.write('[{}] {}\n'.format(ct, i))
            i += 1
            mapped_terms, real_props, covered_query_map = pipeline.run(tag_to_val, covered_query_map)
            mappings = {
                "mapped_terms": [x.to_dict() for x in mapped_terms],
                "real_value_properties": [x.to_dict() for x in real_props]
            }
            all_mappings.append(mappings)
    else:
        p = Pool(processes)
        size = len(tag_to_vals)/processes
        res = []
        for i in range(processes):
            sub_tag_to_vals = tag_to_vals[int(i*size):int((i+1)*size)]
            res.append(p.apply_async(pipeline.run_multiple, (sub_tag_to_vals,)))
        for r in res:
            all_mappings += r.get()

    ct = datetime.datetime.now()
    sys.stderr.write('[{}] Run pipeline on key vals\n'.format(ct, processes))
    outputs = []
    output_for_prediction = []
    vectorizer_f = pr.resource_filename(__name__, join("map_sra_to_ontology", "predict_sample_type", "sample_type_vectorizor.dill"))
    classifier_f = pr.resource_filename(__name__, join("map_sra_to_ontology", "predict_sample_type", "sample_type_classifier.dill"))
    with open(vectorizer_f, "rb") as f:
        vectorizer = dill.load(f)
    with open(classifier_f, "rb") as f:
        model = dill.load(f)
    for tag_to_val, mappings in zip(tag_to_vals, all_mappings):
        result = run_pipeline_on_key_vals(
            tag_to_val, ont_id_to_og, mappings, vectorizer, model)
        outputs.append(result)

    sys.stderr.write('[{}] Writing.\n'.format(ct))
    if output_f.split(".")[-1] == "json":
        output_json = json.dumps(outputs, indent=4, separators=(',', ': '))
        with open(output_f, mode='w') as f:
            f.write(output_json)
    elif output_f.split(".")[-1] == "ttl":
        print_as_turtle(outputs, output_f)
    else:
        print_as_tsv(outputs, tag_to_vals, output_f)

    # with open("for_sample_type_prediction"+output_f, mode='w') as f:
    #     output_json = json.dumps(output_for_prediction,
    #                              indent=4, separators=(',', ': '))
    #     f.write(output_json)
    sys.stderr.write('[{}] Done.\n'.format(ct))


def run_pipeline_on_key_vals(tag_to_val, ont_id_to_og, mapping_data,
                             vectorizer, model):
    mapped_terms = []
    real_val_props = []
    mapped_terms_details = []
    # remove "cell line", "disease", "treatment"
    excluding_term_ids = ["EFO:0000322",
                          "DOID:4",
                          "EFO:0000727",
                          "EFO:0000408",
                          "Orphanet:377788"]

    for mapped_term_data in mapping_data["mapped_terms"]:
        term_id = mapped_term_data["term_id"]
        for ont in list(ont_id_to_og.values()):
            if term_id in ont.get_mappable_term_ids() and term_id not in excluding_term_ids:
                mapped_terms.append(term_id)
                mapped_term_detail = mapped_term_data.copy()
                mapped_term_detail["term_name"] = ont.id_to_term[term_id].name
                mapped_terms_details.append(mapped_term_detail)
                break
    for real_val_data in mapping_data["real_value_properties"]:
        real_val_prop = {
            "unit_id":real_val_data["unit_id"], 
            "value":real_val_data["value"], 
            "property_id":real_val_data["property_id"],
            "original_key":real_val_data["original_key"], 
            "consequent":real_val_data["consequent"], 
            "path_to_mapping":real_val_data["path_to_mapping"]
        }
        real_val_props.append(real_val_prop)

    # Add super-terms of mapped terms to the list of ontology term features
    sup_terms = set()
    for og in list(ont_id_to_og.values()):
        for term_id in mapped_terms:
            sup_terms.update(og.recursive_relationship(term_id, ['is_a', 'part_of']))
    mapped_terms = list(sup_terms)

    predicted, confidence = run_sample_type_predictor.run_sample_type_prediction(
        tag_to_val,
        mapped_terms,
        real_val_props,
        vectorizer,
        model
    )
    # for_sample_type_prediction = {
    #     "tag_to_val": tag_to_val,
    #     "mapped_terms": mapped_terms,
    #     "real_val_props": real_val_props
    # }

    mapping_data = {
        "mapped ontology terms": mapped_terms_details,
        "real-value properties": real_val_props,
        "sample type": predicted,
        "sample-type confidence": confidence
    }

    accession = tag_to_val.get("accession")
    if accession:
        mapping_data["accession"] = accession

    return mapping_data # , for_sample_type_prediction


def print_as_tsv(mappings, tag_to_vals, output_f):  # ont_id_to_og,
    acc_to_kvs = {}
    lines = ""
    for tag_to_val in tag_to_vals:
        acc_to_kvs[tag_to_val["accession"]] = tag_to_val

    for sample in mappings:
        mapped_keys = set()
        for mot in sample["mapped ontology terms"]:
            line = sample["accession"]
            line += "\t" + mot["original_key"]
            line += "\t" + mot["original_value"]
            line += "\t" + mot["term_id"]
            line += "\t" + mot["term_name"]
            line += "\t" + str(mot["consequent"])
            line += "\t" + str(mot["full_length_match"])
            line += "\t" + str(mot["exact_match"])
            line += "\t" + str(mot["match_target"])
            if lines != "":
                lines += "\n"
            lines += line
            mapped_keys.add(mot["original_key"])
        for key in acc_to_kvs[sample["accession"]]:
            if key in ["accession", "taxId"]:
                continue
            if key not in mapped_keys:
                line = sample["accession"]
                line += "\t" + key + "\t" + acc_to_kvs[sample["accession"]][key]
                if lines != "":
                    lines += "\n"
                lines += line

    if output_f == "":
        print(lines)
    else:
        with open(output_f, mode='w') as f:
            f.write(lines)
    return


def print_as_turtle(mappings, output_filename):
    if output_filename == "":
        output_file = sys.stdout
    else:
        output_file = open(output_filename, mode="w")

    with open(pr.resource_filename(__name__, "ont_prefix_to_uri.json")) as f:
        ont_prefix_to_uri = json.load(f)

    g = rdflib.Graph()
    schema = rdflib.Namespace("http://schema.org/")
    provo  = rdflib.Namespace("http://www.w3.org/ns/prov#")
    rdf    = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    xsd    = rdflib.Namespace("http://www.w3.org/2001/XMLSchema#")
    obo    = rdflib.Namespace("http://purl.obolibrary.org/obo/")
    g.namespace_manager.bind("provo", provo)
    g.namespace_manager.bind("obo", obo)
    for sample in mappings:
        sample_uri = rdflib.URIRef("http://identifiers.org/biosample/" + sample["accession"])
        for mot in sample["mapped ontology terms"]:
            term_uri_prefix = ont_prefix_to_uri[mot["term_id"].split(":")[0]]
            mapped_term_uri = term_uri_prefix + mot["term_id"].replace(":", "_")
            bnode1 = rdflib.BNode()
            bnode2 = rdflib.BNode()
            g.add((sample_uri, schema["mainEntity"], bnode1))
            g.add((bnode1, schema["additionalProperty"], bnode2))
            g.add((bnode2, rdf["type"], schema["PropertyValue"]))
            g.add((bnode2, provo["wasGeneratedBy"], rdflib.URIRef("http://ddbj.nig.ac.jp/ontology/BioSamplePlus")))
            g.add((bnode2, schema["name"],
                   rdflib.Literal(mot["original_key"])))
            g.add((bnode2, schema["value"],
                   rdflib.Literal(mot["original_value"])))
            g.add((bnode2, schema["valueReference"],
                   rdflib.URIRef(mapped_term_uri)))
        for rvp in sample["real-value properties"]:
            rvp_value = rvp["value"]
            bnode1 = rdflib.BNode()
            bnode2 = rdflib.BNode()
            g.add((sample_uri, schema["mainEntity"], bnode1))
            g.add((bnode1, schema["additionalProperty"], bnode2))
            g.add((bnode2, rdf["type"], schema["PropertyValue"]))
            g.add((bnode2, provo["wasGeneratedBy"], rdflib.URIRef("http://ddbj.nig.ac.jp/ontology/BioSamplePlus")))
            g.add((bnode2, schema["name"],
                   rdflib.Literal(rvp["original_key"])))
            if rvp["unit_id"] == "missing":
                g.add((bnode2, schema["valueReference"], rdflib.Literal(rvp_value, datatype=xsd["decimal"])))
            else:
                unit_uri_prefix = ont_prefix_to_uri[rvp["unit_id"].split(":")[0]]
                unit_uri = unit_uri_prefix + rvp["unit_id"].replace(":", "_")
                g.add((bnode2, schema["valueReference"],
                       rdflib.Literal(rvp_value, datatype=rdflib.URIRef(unit_uri))))

    print(g.serialize(format="turtle", base="http://schema.org/").decode("utf-8"), file=output_file)
    if output_filename != "":
        output_file.close()
    return


if __name__ == "__main__":
    main()
