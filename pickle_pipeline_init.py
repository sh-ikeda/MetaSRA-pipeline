import pickle
import json
import sys
import pkg_resources as pr
from os.path import join
from optparse import OptionParser
from collections import defaultdict

from map_sra_to_ontology import config
from map_sra_to_ontology import pipeline_components as pc
from map_sra_to_ontology.utils import log_time

resource_package = __name__


def create_pipeline(ont_id_to_og, pipeline_config):
    conf = defaultdict(lambda: False)
    for k in pipeline_config["stages"]:
        conf[k] = pipeline_config["stages"][k]

    stages = []

    log_time("key_val_filt")
    stages.append(pc.KeyValueFilter_Stage())

    log_time("init_tokens_stage")
    stages.append(pc.InitKeyValueTokens_Stage())

    log_time("ngram")
    stages.append(pc.NGram_Stage())

    log_time("lower_stage")
    stages.append(pc.Lowercase_Stage())

    log_time("delimit")
    stages.append(pc.Delimit_Stage("+", conf["replace_to_space"]))
    stages.append(pc.Delimit_Stage("_", conf["replace_to_space"]))
    stages.append(pc.Delimit_Stage("-", conf["replace_to_space"]))
    stages.append(pc.Delimit_Stage("/", conf["replace_to_space"]))

    log_time("spec_lex")
    spec_lex = pc.SpecialistLexicon(config.specialist_lex_location())

    if conf["do_inflec_var"]:
        log_time("inflec_var")
        stages.append(pc.SPECIALISTLexInflectionalVariants(spec_lex))

    if conf["do_spell_var"]:
        log_time("spell_var")
        stages.append(pc.SPECIALISTSpellingVariants(spec_lex))

    if conf["do_man_at_syn"]:
        log_time("man_at_syn")
        stages.append(pc.ManuallyAnnotatedSynonyms_Stage())

    log_time("exact_match")
    target_ogs = [ont_id_to_og[id] for id in ont_id_to_og]
    exact_match = pc.ExactStringMatching_Stage(target_ogs, query_len_thresh=conf["exact_match_thresh"])
    stages.append(exact_match)

    if conf["do_two_char_match"]:
        log_time("two_char_match")
        two_char_json = pr.resource_filename(
            resource_package,
            join("map_sra_to_ontology", "metadata", conf["two_char_match_json"])
        )
        stages.append(pc.TwoCharMappings_Stage(two_char_json))

    log_time("fuzzy_match")
    stages.append(pc.FuzzyStringMatching_Stage(0.1, query_len_thresh=3))

    if conf["do_match_cust_targs"]:
        log_time("match_cust_targs")
        stages.append(pc.ExactMatchCustomTargets_Stage())

    if conf["do_block_cell_line_key"]:
        log_time("block_cell_line_key")
        stages.append(pc.BlockCellLineNonCellLineKey_Stage(ont_id_to_og["4"], cell_line_phrases=[], cell_line_phrases_low_prior=[]))

    log_time("subphrase_linked")
    stages.append(pc.RemoveSubIntervalOfMatchedBlockAncestralLink_Stage())

    log_time("taxid_filter")
    stages.append(pc.FilterMappingsToCellLinesByTaxId_Stage(ont_id_to_og["4"]))

    if conf["do_filt_match_priority"]:
        log_time("filt_match_priority")
        stages.append(pc.FilterOntologyMatchesByPriority_Stage())

    if conf["do_prioritize_exact"]:
        log_time("prioritize_exact")
        stages.append(pc.PrioritizeExactMatchOverFuzzyMatch())

    print(f"# of stages: {len(stages)}", file=sys.stderr)

    return pc.Pipeline(stages)


def main():
    parser = OptionParser()
    parser.add_option("-o", "--output_filename")
    parser.add_option("-i", "--config_filename")
    parser.add_option("-n", "--config_id")

    (options, args) = parser.parse_args()
    output_filename = options.output_filename
    config_filename = options.config_filename
    config_id = options.config_id

    with open(config_filename, "r") as f:
        pipeline_configs = json.load(f)

    pipeline_config = {}
    for conf in pipeline_configs:
        if conf["id"] == config_id:
            pipeline_config = conf
    if len(pipeline_config.keys()) == 0:
        print(f"Error: Invalid config_id: {config_id}", file=sys.stderr)
        exit(1)

    # Load ontologies
    log_time("Loading ontologies")
    # ont_name_to_ont_id = {"CVCL": "4"}
    ont_pickle_filenames = pipeline_config["stages"]["ontologies"]

    ont_id_to_og = {}
    # for ont_name in list(ont_name_to_ont_id.keys()):
    for ont_pickle in ont_pickle_filenames:
        log_time(ont_pickle)
        ont_pickle_path = pr.resource_filename(
            resource_package,
            join("setup_map_sra_to_ontology", "ontology_pickle", ont_pickle)
        )
        with open(ont_pickle_path, "rb") as f:
            og_info = pickle.load(f)
            ont_id_to_og[og_info[1]] = og_info[0]
    pipeline = create_pipeline(ont_id_to_og, pipeline_config)

    log_time("pickle dump")
    with open(output_filename, "wb") as f:
        pickle.dump((pipeline, ont_id_to_og), f)

    log_time("Done.")

if __name__ == "__main__":
    main()
