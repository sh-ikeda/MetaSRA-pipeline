from bktree import BKTree
import load_ontology
import string_metrics

import json
import pickle
from collections import defaultdict
import argparse


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="Build BK-tree for fuzzy matching.\n"
        "Example of ID specification:\n\thuman: 1,2,18,5,7,9,19\n"
        "\tarabidopsis: 20,21")
    parser.add_argument("-i", "--ids", required=True,
                        help="comma separated list of ontology IDs.")
    parser.add_argument("-j", "--json_filename", required=True,
                        help="filename of output json."
                        "'fuzzy_match_string_data_SPECIES.json' is recommended.")
    parser.add_argument("-p", "--pickle_filename", required=True,
                        help="filename of output pickle"
                        "'fuzzy_match_bk_tree_SPECIES.pickle' is recommended.")
    args = parser.parse_args()

    og_ids = args.ids.split(",")
    ogs = [load_ontology.load(x)[0] for x in og_ids]
    str_to_terms = defaultdict(lambda: [])

    print("Gathering all term string identifiers in ontologies...")
    string_identifiers = set()
    for og in ogs:
        for id, term in og.id_to_term.items():
            str_to_terms[term.name].append([term.id, "TERM_NAME"])
            string_identifiers.add(term.name)
            for syn in term.synonyms:
                str_to_terms[syn.syn_str].append(
                    [term.id, "SYNONYM_%s" % syn.syn_type])
                string_identifiers.add(syn.syn_str)

    print("Building the BK-Tree...")
    bk_tree = BKTree(string_metrics.bag_dist_multiset, string_identifiers)

    with open(args.pickle_filename, "wb") as f:
        pickle.dump(bk_tree, f)

    with open(args.json_filename, "w") as f:
        f.write(json.dumps(str_to_terms, indent=4, separators=(',', ': ')))


if __name__ == "__main__":
    main()
