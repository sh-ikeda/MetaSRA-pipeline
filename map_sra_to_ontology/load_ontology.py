import pkg_resources as pr
import json

# from . import config
# from . import ontology_graph
from map_sra_to_ontology import config
from map_sra_to_ontology import ontology_graph


def load(ontology_index):
    resource_package = __name__
    config_f = pr.resource_filename(resource_package, "./ontology_configurations.json")
    with open(config_f, "r") as f:
        j = json.load(f)
    ont_config = j[ontology_index]

    include_ontologies = ont_config["included_ontology_projects"]
    restrict_to_idspaces = ont_config["id_spaces"]
    is_restrict_roots = ont_config["restrict_to_specific_subgraph"]
    restrict_to_roots = ont_config["subgraph_roots"] if is_restrict_roots else None
    exclude_terms = ont_config["exclude_terms"]

    ont_to_loc = {
        x: y
        for x, y in config.ontology_name_to_location().items()
        if x in include_ontologies
    }
    og = ontology_graph.build_ontology(
        ont_to_loc,
        ontology_index,
        ont_config["description"],
        restrict_to_idspaces=restrict_to_idspaces,
        include_obsolete=False,
        restrict_to_roots=restrict_to_roots,
        exclude_terms=exclude_terms,
    )

    return og, include_ontologies, restrict_to_roots


def main():
    og, i, r = load("4")
    print(og.id_to_term["CVCL:C792"])
    # og, i, r = load("19")
    # print(og.id_to_term["Orphanet:95"])
    # og, i, r = load("16")
    # print(og.id_to_term["EFO:0000572"])
    # og, i, r = load("12")
    # print(og.id_to_term["NCBITaxon:9606"])
    og, i, r = load("21")  # EFO arabi

    dic = dict()
    for t in og.id_to_term.values():
        dic[t.id] = t.name
    with open("output.json", "w") as f:
        json.dump(dic, f, indent=4)


if __name__ == "__main__":
    main()
