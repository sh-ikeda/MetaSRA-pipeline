########################################################################
#
# Run the ontology mapping pipeline on a set of key-value pairs
# that describe a biological sample
#
########################################################################

from optparse import OptionParser
import json
import sys
from multiprocessing import Pool
import pkg_resources as pr
from map_sra_to_ontology.utils import log_time
import rdflib
import urllib
import pickle


def main():
    parser = OptionParser()
    parser.add_option(
        "-f",
        "--key_value_file",
        help="JSON file storing key-value pairs describing sample",
        dest="input_filename",
    )
    parser.add_option(
        "-o",
        "--output",
        help="Output filename",
        dest="output_filename",
        type="str",
        default="",
    )
    parser.add_option(
        "-i",
        "--init",
        help="init pickle file",
        dest="init_pickle",
        default=pr.resource_filename(__name__, "pipeline_init.pickle"),
    )
    parser.add_option(
        "-n",
        "--processes",
        help="# of processes",
        dest="processes",
        type="int",
        default=1,
    )
    parser.add_option(
        "-d", "--debug", help="debug mode", dest="debug_mode", action="store_true"
    )
    parser.add_option("-t", "--test", help="test mode", dest="test_mode", action="store_true")
    parser.add_option(
        "-c",
        "--cvcl",
        help="Include CVCL summary in tsv output",
        dest="include_cvcl",
        action="store_true",
    )
    (options, args) = parser.parse_args()

    input_f = options.input_filename
    output_f = options.output_filename
    init_pickle = options.init_pickle
    processes = options.processes
    debug_mode = options.debug_mode
    test_mode = options.test_mode
    include_cvcl = options.include_cvcl

    # Map key-value pairs to ontologies
    with open(input_f, "r", encoding="utf-8") as f:
        biosample_json = json.load(f)

    tag_to_vals = []
    log_time("Parsing BioSample JSON")
    if test_mode:
        for tag_to_val in biosample_json:
            entry = {}
            entry["accession"] = tag_to_val["sample_accession"]
            entry["taxId"] = "9606"
            for k in tag_to_val["attributes"]:
                val = tag_to_val["attributes"][k]
                if k != "accession":
                    entry[k] = [val]
            tag_to_vals.append(entry)
    else:
        for tag_to_val in biosample_json:
            entry = {}
            entry["accession"] = tag_to_val["accession"]
            entry["taxId"] = str(tag_to_val.get("taxId", ""))
            for k in tag_to_val["characteristics"]:
                if k in ["accession", "taxId"]:
                    continue
                vals = []
                for val in tag_to_val["characteristics"][k]:
                    vals.append(val["text"])
                if len(vals) != 0:
                    entry[k] = vals
            tag_to_vals.append(entry)

    # Load ontologies
    log_time("Initializing pipeline.")
    with open(init_pickle, "rb") as f:
        vars = pickle.load(f)
        pipeline = vars[0]
        ont_id_to_og = vars[1]

    all_mappings = []
    log_time(f"Mapping with {processes} processes")
    if processes == 1:
        i = 0
        n_of_samples = str(len(tag_to_vals))
        covered_query_map = dict()
        for tag_to_val in tag_to_vals:
            if i % 10 == 0 and debug_mode:
                log_time(str(i) + " / " + str(n_of_samples))
            i += 1
            mapped_terms, real_props, covered_query_map = pipeline.run(tag_to_val, covered_query_map)
            mappings = {
                "mapped_terms": [x.to_dict() for x in mapped_terms],
                "real_value_properties": [x.to_dict() for x in real_props],
            }
            all_mappings.append(mappings)
    else:
        p = Pool(processes)
        size = len(tag_to_vals) / processes
        res = []
        for i in range(processes):
            sub_tag_to_vals = tag_to_vals[int(i * size) : int((i + 1) * size)]
            res.append(p.apply_async(pipeline.run_multiple, (sub_tag_to_vals,)))
        for r in res:
            all_mappings += r.get()

    log_time("Run pipeline on key vals")
    outputs = []
    for tag_to_val, mappings in zip(tag_to_vals, all_mappings):
        result = run_pipeline_on_key_vals(tag_to_val, ont_id_to_og, mappings)
        outputs.append(result)

    log_time("Writing.")
    if output_f.split(".")[-1] == "json":
        output_json = json.dumps(outputs, indent=4, separators=(",", ": "))
        with open(output_f, mode="w") as f:
            f.write(output_json)
    elif output_f.split(".")[-1] == "ttl":
        print_as_turtle(outputs, output_f)
    else:
        print_as_tsv(outputs, tag_to_vals, output_f, ont_id_to_og, include_cvcl)

    log_time("Done.")


def run_pipeline_on_key_vals(tag_to_val, ont_id_to_og, mapping_data):
    mapped_terms = []
    real_val_props = []
    mapped_terms_details = []

    for mapped_term_data in mapping_data["mapped_terms"]:
        term_id = mapped_term_data["term_id"]
        for ont in list(ont_id_to_og.values()):
            if term_id in ont.get_mappable_term_ids():
                mapped_terms.append(term_id)
                mapped_term_detail = mapped_term_data.copy()
                mapped_term_detail["term_name"] = ont.id_to_term[term_id].name
                mapped_terms_details.append(mapped_term_detail)
                break

    for real_val_data in mapping_data["real_value_properties"]:
        real_val_prop = {
            "unit_id": real_val_data["unit_id"],
            "value": real_val_data["value"],
            "property_id": real_val_data["property_id"],
            "original_key": real_val_data["original_key"],
            "consequent": real_val_data["consequent"],
            "path_to_mapping": real_val_data["path_to_mapping"],
        }
        real_val_props.append(real_val_prop)

    # Add super-terms of mapped terms to the list of ontology term features
    sup_terms = set()
    for og in list(ont_id_to_og.values()):
        for term_id in mapped_terms:
            sup_terms.update(og.recursive_relationship(term_id, ["is_a", "part_of"]))
    mapped_terms = list(sup_terms)

    mapping_data = {
        "mapped ontology terms": mapped_terms_details,
        "real-value properties": real_val_props,
    }

    accession = tag_to_val.get("accession")
    if accession:
        mapping_data["accession"] = accession

    return mapping_data


def print_as_tsv(mappings, tag_to_vals, output_f, ont_id_to_og, include_cvcl=False):  # ont_id_to_og,
    acc_to_kvs = {}
    lines = ""
    for tag_to_val in tag_to_vals:
        acc_to_kvs[tag_to_val["accession"]] = tag_to_val

    for sample in mappings:
        mapped_keys = set()
        for mot in sample["mapped ontology terms"]:
            line = [
                sample["accession"],
                mot["original_key"],
                mot["original_value"],
                mot["term_id"],
                mot["term_name"],
                str(mot["consequent"]),
                str(mot["full_length_match"]),
                str(mot["exact_match"]),
                str(mot["match_target"]),
                # sample["sample type"],
                # str(sample["sample-type confidence"]),
            ]
            if include_cvcl:
                cvcl_summary = ""
                term_id = mot["term_id"]
                if term_id.startswith("CVCL:"):
                    term = ont_id_to_og["4"].id_to_term[term_id]
                    cvcl_summary_dict = {
                        "id": term_id,
                        "name": term.name,
                        "synonyms": [{"name": x.syn_str, "type": x.syn_type} for x in list(term.synonyms)],
                        "subsets": list(term.subsets),
                        "xrefs": [],
                        "xrefs_comments": [],
                    }
                    for i in range(len(term.xrefs)):
                        xref = term.xrefs[i]
                        xref_comment = term.xrefs_comments[i]
                        if xref.split(":")[0] in ["NCIt", "NCBI_TaxID"]:
                            cvcl_summary_dict["xrefs"].append(xref)
                            cvcl_summary_dict["xrefs_comments"].append(xref_comment)
                    cvcl_summary = json.dumps(cvcl_summary_dict)
                line.append(cvcl_summary)
            if lines != "":
                lines += "\n"
            lines += "\t".join(line)
            mapped_keys.add(mot["original_key"])

        ## Attributes not mapped to any terms
        for key in acc_to_kvs[sample["accession"]]:
            if key in ["accession", "taxId"]:
                continue
            if key not in mapped_keys:
                for v in acc_to_kvs[sample["accession"]][key]:
                    line = sample["accession"]
                    line += "\t" + key + "\t" + v
                    if lines != "":
                        lines += "\n"
                    lines += line

    if output_f == "":
        print(lines)
    else:
        with open(output_f, mode="w") as f:
            f.write(lines)
    return


def print_as_turtle(mappings, output_filename):
    if output_filename == "":
        output_file = sys.stdout
    else:
        output_file = open(output_filename, mode="w")

    with open(pr.resource_filename(__name__, "map_sra_to_ontology", "ont_prefix_to_uri.json")) as f:
        ont_prefix_to_uri = json.load(f)

    g = rdflib.Graph()
    ddbjont = rdflib.Namespace("http://ddbj.nig.ac.jp/ontologies/biosample/")
    schema = rdflib.Namespace("http://schema.org/")
    provo = rdflib.Namespace("http://www.w3.org/ns/prov#")
    rdf = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    xsd = rdflib.Namespace("http://www.w3.org/2001/XMLSchema#")
    obo = rdflib.Namespace("http://purl.obolibrary.org/obo/")
    ddbj = rdflib.Namespace("http://ddbj.nig.ac.jp/biosample/")
    g.namespace_manager.bind("ddbjont", ddbjont)
    g.namespace_manager.bind("provo", provo)
    g.namespace_manager.bind("obo", obo)
    g.namespace_manager.bind("ddbj", ddbj)
    g.namespace_manager.bind("", schema)
    sample_type_dict = {
        "cell_line": "CellLine",
        "primary_cells": "PrimaryCell",
        "stem_cells": "StemCell",
        "tissue": "Tissue",
        "in_vitro_differentiated_cells": "InVitroDifferintiatedCells",
        "induced_pluripotent_stem_cells": "IPSCellLine",
    }
    for sample in mappings:
        # sample_uri_str = "http://identifiers.org/biosample/" + sample["accession"]
        # sample_type_uri = rdflib.URIRef(sample_uri_str + "#AnnotatedSampleType")
        sample_type_uri = ddbj[sample["accession"] + "#" + "AnnotatedSampleType"]
        g.add(
            (ddbj[sample["accession"]], schema["additionalProperty"], sample_type_uri)
        )
        g.add((sample_type_uri, rdf["type"], schema["PropertyValue"]))
        g.add(
            (sample_type_uri, schema["name"], rdflib.Literal("annotated sample type"))
        )
        g.add((sample_type_uri, schema["value"], rdflib.Literal(sample["sample type"])))
        g.add(
            (
                sample_type_uri,
                schema["valueReference"],
                ddbjont[sample_type_dict[sample["sample type"]]],
            )
        )
        g.add(
            (
                sample_type_uri,
                ddbjont["annotationConfidence"],
                rdflib.Literal(
                    sample["sample-type confidence"], datatype=xsd["decimal"]
                ),
            )
        )
        g.add(
            (
                sample_type_uri,
                provo["wasAttributedTo"],
                ddbjont["BioSamplePlusAnnotation"],
            )
        )
        for mot in sample["mapped ontology terms"]:
            term_uri_prefix = ont_prefix_to_uri[mot["term_id"].split(":")[0]]
            mapped_term_uri = rdflib.URIRef(
                term_uri_prefix + mot["term_id"].replace(":", "_")
            )
            # property_value_uri = rdflib.URIRef(
            #     sample_uri_str + "#" + urllib.parse.quote(mot["original_key"]))
            property_value_uri = ddbj[
                sample["accession"] + "#" + urllib.parse.quote_plus(mot["original_key"])
            ]
            g.add((property_value_uri, schema["valueReference"], mapped_term_uri))
            g.add(
                (
                    property_value_uri,
                    provo["wasAttributedTo"],
                    ddbjont["BioSamplePlusAnnotation"],
                )
            )

        for rvp in sample["real-value properties"]:
            rvp_value = rvp["value"]
            property_value_uri = ddbj[
                sample["accession"] + "#" + urllib.parse.quote_plus(mot["original_key"])
            ]
            # property_value_uri = rdflib.URIRef(
            #     sample_uri_str + "#" + urllib.parse.quote(rvp["original_key"]))
            g.add(
                (
                    property_value_uri,
                    provo["wasAttributedTo"],
                    ddbjont["BioSamplePlusAnnotation"],
                )
            )
            if rvp["unit_id"] == "missing":
                g.add(
                    (
                        property_value_uri,
                        schema["valueReference"],
                        rdflib.Literal(rvp_value, datatype=xsd["decimal"]),
                    )
                )
            else:
                unit_uri_prefix = ont_prefix_to_uri[rvp["unit_id"].split(":")[0]]
                unit_uri = unit_uri_prefix + rvp["unit_id"].replace(":", "_")
                g.add(
                    (
                        property_value_uri,
                        schema["valueReference"],
                        rdflib.Literal(rvp_value, datatype=rdflib.URIRef(unit_uri)),
                    )
                )

    # ttl_str = g.serialize(format="turtle", base="http://schema.org/").decode("utf-8")
    ttl_str = g.serialize(format="turtle").decode("utf-8")
    # p1 = re.compile("^<http://ddbj.nig.ac.jp/biosample/([^#]+)#([^>]+)>", flags=re.MULTILINE)
    # #pattern = re.compile("(ddbj:[^ _]+)_REPLACE_")
    # ttl_str = p1.sub(r"ddbj:\1\\#\2", ttl_str)
    # p2 = re.compile("^ddbj:[^ ]+ ", flags=re.MULTILINE)
    # spans = list(p2.finditer(ttl_str))
    # i = 0
    # p3 = re.compile("\+")
    # for sp in [s.span() for s in spans]:
    #     repl, n = p3.subn("\\+", ttl_str[sp[0]+i:sp[1]+i])
    #     ttl_str = ttl_str[:sp[0]+i] + repl + ttl_str[sp[1]+i:]
    #     i += n
    # # ttl_str = pattern.sub(r"\\+", ttl_str)
    # ttl_str = "@prefix ddbj: <http://ddbj.nig.ac.jp/biosample/> .\n" + ttl_str
    print(ttl_str, file=output_file)
    if output_filename != "":
        output_file.close()
    return


if __name__ == "__main__":
    main()
