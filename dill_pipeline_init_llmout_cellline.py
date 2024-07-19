import pickle

from map_sra_to_ontology import load_ontology
from map_sra_to_ontology import config
from map_sra_to_ontology import pipeline_components as pc
from map_sra_to_ontology.utils import log_time


def p_48(ont_id_to_og):
    log_time("init_tokens_stage")
    init_tokens_stage = pc.InitKeyValueTokens_Stage()
    log_time("ngram")
    ngram = pc.NGram_Stage()
    log_time("lower_stage")
    lower_stage = pc.Lowercase_Stage()

    log_time("delimit")
    delimit_plus = pc.Delimit_Stage("+")
    delimit_underscore = pc.Delimit_Stage("_")
    delimit_dash = pc.Delimit_Stage("-")
    delimit_slash = pc.Delimit_Stage("/")

    log_time("exact_match")
    # target_og_ids = ["1", "2", "4", "5", "7", "9", "19", "20", "21"]
    target_og_ids = ["4"]
    target_ogs = [ont_id_to_og[id] for id in target_og_ids]
    # exact_match = pc.ExactStringMatchingToSpecificOntology_Stage(target_ogs, query_len_thresh=3)
    exact_match = pc.ExactStringMatchingToSpecificOntology_Stage(target_ogs)
    log_time("fuzzy_match")
    fuzzy_match = pc.FuzzyStringMatching_Stage(0.1, query_len_thresh=3)
    # log_time("two_char_match")
    # two_char_match = pc.TwoCharMappings_Stage()
    log_time("subphrase_linked")
    subphrase_linked = pc.RemoveSubIntervalOfMatchedBlockAncestralLink_Stage()
    log_time("taxid_filter")
    taxid_filter = pc.FilterMappingsToCellLinesByTaxId_Stage(ont_id_to_og["4"])

    stages = [
        init_tokens_stage,
        ngram,
        lower_stage,
        delimit_plus,
        delimit_underscore,
        delimit_dash,
        delimit_slash,
        exact_match,
        # two_char_match,
        fuzzy_match,
        subphrase_linked,
        taxid_filter,
    ]
    return pc.Pipeline(stages)


# Load ontologies
log_time("Loading ontologies")
ont_name_to_ont_id = {
    "CVCL": "4"
}
# ont_id_to_og = {x: load_ontology.load(x)[0]
#                 for x in list(ont_name_to_ont_id.values())}
ont_id_to_og = {}
for ont_name in list(ont_name_to_ont_id.keys()):
    id = ont_name_to_ont_id[ont_name]
    ont_id_to_og[id] = load_ontology.load(id)[0]
pipeline = p_48(ont_id_to_og)

log_time("pickle dump")
with open("pipeline_init_llmout_cellline.pickle", "wb") as f:
    pickle.dump((pipeline, ont_id_to_og), f)

log_time("Done.")
