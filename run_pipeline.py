########################################################################
#
# Run the ontology mapping pipeline on a set of key-value pairs
# that describe a biological sample
#
########################################################################

from optparse import OptionParser
import json
import sys
import dill
from multiprocessing import Pool
import pkg_resources as pr
from map_sra_to_ontology.utils import log_time
from os.path import join
import rdflib
import urllib
import pickle
import xlsxwriter


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
        help="init dill file",
        dest="init_dill",
        # default=pr.resource_filename(__name__, "pipeline_init.dill"))
        default=pr.resource_filename(__name__, "pipeline_init.pickle"),
    )
    parser.add_option(
        "-k",
        "--keywords",
        help="specified mapping keywords json",
        dest="keywords_filename",
        type="str",
        default="",
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
    parser.add_option(
        "-r",
        "--disable_cell_line_restriction",
        help="Disable restriction of attributes allowed to be mapped to CVCL term",
        dest="disable_cell_line_restriction",
        action="store_true",
        default=False,
    )
    parser.add_option(
        "-l",
        "--disable_long_text_restriction",
        help="Disable restriction of attributes with a value of length longer than 100 chars",
        dest="disable_long_text_restriction",
        action="store_true",
        default=False,
    )
    (options, args) = parser.parse_args()

    input_f = options.input_filename
    output_f = options.output_filename
    init_dill = options.init_dill
    processes = options.processes
    debug_mode = options.debug_mode
    keywords_f = options.keywords_filename
    test_mode = options.test_mode
    include_cvcl = options.include_cvcl

    pipeline_options = {
        "disable_cell_line_restriction": options.disable_cell_line_restriction,
        "disable_long_text_restriction": options.disable_long_text_restriction,
    }
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
    # dill.load_session(init_dill)
    with open(init_dill, "rb") as f:
        # vars = dill.load(f)
        vars = pickle.load(f)
        pipeline = vars[0]
        ont_id_to_og = vars[1]

    # TWO_CHAR_MAPPINGS_JSON = "/mnt/c/Users/togotv_dell1/work/biosample/MetaSRA-pipeline/map_sra_to_ontology/metadata/two_char_mappings.json"
    if keywords_f != "":
        with open(keywords_f, "r") as f:
            pipeline.stages[14].str_to_mappings = json.load(f)

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
            mapped_terms, real_props, covered_query_map = pipeline.run(
                tag_to_val, covered_query_map, pipeline_options
            )
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
    output_for_prediction = []
    vectorizer_f = pr.resource_filename(
        __name__,
        join(
            "map_sra_to_ontology", "predict_sample_type", "sample_type_vectorizor.dill"
        ),
    )
    classifier_f = pr.resource_filename(
        __name__,
        join(
            "map_sra_to_ontology", "predict_sample_type", "sample_type_classifier.dill"
        ),
    )
    with open(vectorizer_f, "rb") as f:
        vectorizer = dill.load(f)
    with open(classifier_f, "rb") as f:
        model = dill.load(f)
    for tag_to_val, mappings in zip(tag_to_vals, all_mappings):
        result = run_pipeline_on_key_vals(
            tag_to_val, ont_id_to_og, mappings, vectorizer, model
        )
        outputs.append(result)

    log_time("Writing.")
    if output_f.split(".")[-1] == "json":
        output_json = json.dumps(outputs, indent=4, separators=(",", ": "))
        with open(output_f, mode="w") as f:
            f.write(output_json)
    elif output_f.split(".")[-1] == "ttl":
        print_as_turtle(outputs, output_f)
    elif output_f.split(".")[-1] == "xlsx":
        print_as_xlsx(outputs, output_f)
    else:
        print_as_tsv(outputs, tag_to_vals, output_f, ont_id_to_og, include_cvcl)

    # with open("for_sample_type_prediction"+output_f, mode='w') as f:
    #     output_json = json.dumps(output_for_prediction,
    #                              indent=4, separators=(',', ': '))
    #     f.write(output_json)
    log_time("Done.")


def run_pipeline_on_key_vals(tag_to_val, ont_id_to_og, mapping_data, vectorizer, model):
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

    # predicted, confidence = run_sample_type_predictor.run_sample_type_prediction(
    #     tag_to_val, mapped_terms, real_val_props, vectorizer, model
    # )
    # for_sample_type_prediction = {
    #     "tag_to_val": tag_to_val,
    #     "mapped_terms": mapped_terms,
    #     "real_val_props": real_val_props
    # }

    mapping_data = {
        "mapped ontology terms": mapped_terms_details,
        "real-value properties": real_val_props,
        # "sample type": predicted,
        # "sample-type confidence": confidence,
    }

    accession = tag_to_val.get("accession")
    if accession:
        mapping_data["accession"] = accession

    return mapping_data  # , for_sample_type_prediction


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

    with open(pr.resource_filename(__name__, "ont_prefix_to_uri.json")) as f:
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


def print_as_xlsx(mappings, output_filename):
    book = xlsxwriter.Workbook(output_filename)
    sheet = book.add_worksheet()

    sheet.write(0, 0, "accession")
    sheet.write(0, 1, "original_key")
    sheet.write(0, 2, "original_value")
    sheet.write(0, 3, "mapped_term_id")
    sheet.write(0, 4, "mapped_term_name")
    sheet.write(0, 5, "full_length_match")
    sheet.write(0, 6, "exact_match")
    sheet.write(0, 7, "mapped_term_uri")
    sheet.write(0, 8, "OLS_search")

    row_i = 1
    for sample in mappings:
        if len(sample["mapped ontology terms"]) == 0:
            sheet.write(row_i, 0, sample["accession"])
            row_i += 1
        for mt in sample["mapped ontology terms"]:
            font = book.add_format({"color": "red", "bold": True})
            sheet.write(row_i, 0, sample["accession"])
            sheet.write(row_i, 1, mt["original_key"])
            start = mt["origin_pos"][0]
            end = mt["origin_pos"][1]
            if start == 0 and end == len(mt["original_value"]):
                sheet.write(row_i, 2, mt["original_value"][start:end], font)
            elif start == 0:
                sheet.write_rich_string(
                    row_i,
                    2,
                    font,
                    mt["original_value"][start:end],
                    mt["original_value"][end:],
                )
            elif end == len(mt["original_value"]):
                sheet.write_rich_string(
                    row_i,
                    2,
                    mt["original_value"][0:start],
                    font,
                    mt["original_value"][start:end],
                )
            else:
                sheet.write_rich_string(
                    row_i,
                    2,
                    mt["original_value"][0:start],
                    font,
                    mt["original_value"][start:end],
                    mt["original_value"][end:],
                )
            sheet.write(row_i, 3, mt["term_id"])
            sheet.write(row_i, 4, mt["term_name"])
            sheet.write(row_i, 5, mt["full_length_match"])
            sheet.write(row_i, 6, mt["exact_match"])

            ont = mt["term_id"].split(":")[0]
            if ont == "EFO":
                sheet.write_url(
                    row_i,
                    7,
                    "http://www.ebi.ac.uk/efo/" + mt["term_id"].replace(":", "_"),
                )
            elif ont == "Orphanet":
                sheet.write_url(
                    row_i,
                    7,
                    "http://www.orpha.net/ORDO/" + mt["term_id"].replace(":", "_"),
                )
            elif ont == "CVCL":
                sheet.write_url(
                    row_i,
                    7,
                    "http://web.expasy.org/cellosaurus/"
                    + mt["term_id"].replace(":", "_"),
                )
            else:
                sheet.write_url(
                    row_i,
                    7,
                    "http://purl.obolibrary.org/obo/" + mt["term_id"].replace(":", "_"),
                )

            sheet.write_url(
                row_i,
                8,
                "https://www.ebi.ac.uk/ols/search?q="
                + urllib.parse.quote_plus(mt["original_value"].replace("_", " ")),
            )
            row_i += 1

    book.close()


if __name__ == "__main__":
    main()
