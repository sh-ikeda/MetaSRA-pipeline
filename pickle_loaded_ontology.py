import pickle
from optparse import OptionParser

from map_sra_to_ontology import load_ontology
from map_sra_to_ontology.utils import log_time


def main():
    parser = OptionParser()
    parser.add_option("-o", "--output_filename")
    parser.add_option("-i", "--ontology_index")
    parser.add_option("-l", "--include_lowercase", action="store_true")

    (options, args) = parser.parse_args()
    output_filename = options.output_filename
    ontology_index = options.ontology_index
    include_lowercase = options.include_lowercase

    log_time("Loading ontologies")
    og, i, r = load_ontology.load(str(ontology_index), include_lowercase)

    log_time("pickle dump")
    with open(output_filename, "wb") as f:
        pickle.dump((og, i, r), f)
        log_time("Done.")


if __name__ == "__main__":
    main()
