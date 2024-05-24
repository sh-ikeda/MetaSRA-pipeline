import pickle

from map_sra_to_ontology import load_ontology
from map_sra_to_ontology import config
from map_sra_to_ontology import pipeline_components as pc
from map_sra_to_ontology.utils import log_time


def p_48(ont_id_to_og):
    log_time("spec_lex")
    spec_lex = pc.SpecialistLexicon(config.specialist_lex_location())
    log_time("inflec_var")
    inflec_var = pc.SPECIALISTLexInflectionalVariants(spec_lex)
    log_time("spell_var")
    spell_var = pc.SPECIALISTSpellingVariants(spec_lex)
    log_time("key_val_filt")
    key_val_filt = pc.KeyValueFilter_Stage()
    log_time("init_tokens_stage")
    init_tokens_stage = pc.InitKeyValueTokens_Stage()
    log_time("ngram")
    ngram = pc.NGram_Stage()
    log_time("lower_stage")
    lower_stage = pc.Lowercase_Stage()
    log_time("man_at_syn")
    man_at_syn = pc.ManuallyAnnotatedSynonyms_Stage()
    # infer_cell_line = pc.InferCellLineTerms_Stage()
    log_time("prop_spec_syn")
    prop_spec_syn = pc.PropertySpecificSynonym_Stage()
    # infer_dev_stage = pc.ImpliedDevelopmentalStageFromAge_Stage()
    log_time("linked_super")
    linked_super = pc.LinkedTermsOfSuperterms_Stage()
    # cell_culture = pc.ConsequentCulturedCell_Stage()
    log_time("filt_match_priority")
    filt_match_priority = pc.FilterOntologyMatchesByPriority_Stage()
    log_time("real_val")
    real_val = pc.ExtractRealValue_Stage()
    log_time("match_cust_targs")
    match_cust_targs = pc.ExactMatchCustomTargets_Stage()
    # cust_conseq = pc.CustomConsequentTerms_Stage()
    log_time("delimit")
    delimit_plus = pc.Delimit_Stage("+")
    delimit_underscore = pc.Delimit_Stage("_")
    delimit_dash = pc.Delimit_Stage("-")
    delimit_slash = pc.Delimit_Stage("/")
    log_time("block_cell_line_key")
    block_cell_line_key = pc.BlockCellLineNonCellLineKey_Stage(ont_id_to_og["4"])
    log_time("subphrase_linked")
    subphrase_linked = pc.RemoveSubIntervalOfMatchedBlockAncestralLink_Stage()
    # cellline_to_implied_disease = pc.CellLineToImpliedDisease_Stage()
    log_time("acr_to_expan")
    acr_to_expan = pc.AcronymToExpansion_Stage()
    # exact_match = pc.ExactStringMatching_Stage(["1", "2", "4", "5", "7", "8", "9"], query_len_thresh=3)
    log_time("exact_match")
    target_og_ids = ["1", "2", "4", "5", "7", "9", "19"]
    target_ogs = [ont_id_to_og[id] for id in target_og_ids]
    exact_match = pc.ExactStringMatching_Stage(target_ogs, query_len_thresh=3)
    log_time("fuzzy_match")
    fuzzy_match = pc.FuzzyStringMatching_Stage(0.1, query_len_thresh=3)
    log_time("two_char_match")
    two_char_match = pc.TwoCharMappings_Stage()
    log_time("time_unit")
    time_unit = pc.ParseTimeWithUnit_Stage()
    log_time("prior_spec_match")
    prior_spec_match = pc.PrioritizeSpecificMatching_Stage("", [])
    log_time("taxid_filter")
    taxid_filter = pc.FilterMappingsToCellLinesByTaxId_Stage(ont_id_to_og["4"])
    log_time("filter_ambiguous")
    filter_ambiguous = pc.FilterMappingsFromAmbiguousAttributes_Stage()
    log_time("remove_non_specific")
    remove_non_specific = pc.RemoveNonSpecificTerms_Stage()

    stages = [
        key_val_filt,
        init_tokens_stage,
        ngram,
        lower_stage,
        delimit_plus,
        delimit_underscore,
        delimit_dash,
        delimit_slash,
        inflec_var,
        spell_var,
        man_at_syn,
        acr_to_expan,
        time_unit,
        exact_match,
        two_char_match,
        prop_spec_syn,
        fuzzy_match,
        match_cust_targs,
        block_cell_line_key,
        filter_ambiguous,
        linked_super,
        # cellline_to_implied_disease,
        subphrase_linked,
        # cust_conseq,
        real_val,
        taxid_filter,
        filt_match_priority,
        prior_spec_match,
        remove_non_specific
        # infer_cell_line,
        # infer_dev_stage,
        # cell_culture
    ]
    # return pc.Pipeline(stages, defaultdict(lambda: 1.0))
    return pc.Pipeline(stages)


# Load ontologies
log_time("Loading ontologies")
ont_name_to_ont_id = {
    "UBERON": "12",
    "CL": "1",
    "DOID": "2",
    "EFO": "16",
    "CVCL": "4",
    "ORDO": "19",
    "UBERON_all": "5",
    "UO": "7",
    "EFO_all": "9",
}
# ont_id_to_og = {x: load_ontology.load(x)[0]
#                 for x in list(ont_name_to_ont_id.values())}
ont_id_to_og = {}
for ont_name in list(ont_name_to_ont_id.keys()):
    log_time(ont_name)
    id = ont_name_to_ont_id[ont_name]
    ont_id_to_og[id] = load_ontology.load(id)[0]
pipeline = p_48(ont_id_to_og)

del (
    ont_name_to_ont_id["UBERON_all"],
    ont_name_to_ont_id["UO"],
    ont_name_to_ont_id["EFO_all"],
)
del ont_id_to_og["5"], ont_id_to_og["7"], ont_id_to_og["9"]

log_time("pickle dump")
with open("pipeline_init.pickle", "wb") as f:
    pickle.dump((pipeline, ont_id_to_og), f)

log_time("Done.")
