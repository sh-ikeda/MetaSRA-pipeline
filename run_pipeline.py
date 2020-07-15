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
    for tag_to_val, mappings in zip(tag_to_vals, all_mappings):
        outputs.append(run_pipeline_on_key_vals(tag_to_val,
                                                ont_id_to_og,
                                                mappings))

    sys.stderr.write('[{}] Writing.\n'.format(ct))
    if output_f.split(".")[-1] == "json":
        output_json = json.dumps(outputs, indent=4, separators=(',', ': '))
        with open(output_f, mode='w') as f:
            f.write(output_json)
    else:
        print_as_tsv(outputs, tag_to_vals, output_f)
    sys.stderr.write('[{}] Done.\n'.format(ct))


def run_pipeline_on_key_vals(tag_to_val, ont_id_to_og, mapping_data):
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

    mapping_data = {
        "mapped ontology terms": mapped_terms_details,
        "real-value properties": real_val_props
    }

    accession = tag_to_val.get("accession")
    if accession:
        mapping_data["accession"] = accession

    return mapping_data


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


if __name__ == "__main__":
    main()
