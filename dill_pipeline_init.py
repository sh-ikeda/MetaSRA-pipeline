
import sys
import dill
from collections import defaultdict

import map_sra_to_ontology
from map_sra_to_ontology import ontology_graph
from map_sra_to_ontology import load_ontology
from map_sra_to_ontology import predict_sample_type
from map_sra_to_ontology import config
from map_sra_to_ontology import pipeline_components as pc


def dd_init():
    return 1.0


def p_48():
    spec_lex = pc.SpecialistLexicon(config.specialist_lex_location())
    inflec_var = pc.SPECIALISTLexInflectionalVariants(spec_lex)
    spell_var = pc.SPECIALISTSpellingVariants(spec_lex)
    key_val_filt = pc.KeyValueFilter_Stage()
    init_tokens_stage = pc.InitKeyValueTokens_Stage()
    ngram = pc.NGram_Stage()
    lower_stage = pc.Lowercase_Stage()
    man_at_syn = pc.ManuallyAnnotatedSynonyms_Stage()
    #infer_cell_line = pc.InferCellLineTerms_Stage()
    prop_spec_syn = pc.PropertySpecificSynonym_Stage()
    #infer_dev_stage = pc.ImpliedDevelopmentalStageFromAge_Stage()
    linked_super = pc.LinkedTermsOfSuperterms_Stage()
    #cell_culture = pc.ConsequentCulturedCell_Stage()
    filt_match_priority = pc.FilterOntologyMatchesByPriority_Stage()
    real_val = pc.ExtractRealValue_Stage()
    match_cust_targs = pc.ExactMatchCustomTargets_Stage()
    #cust_conseq = pc.CustomConsequentTerms_Stage()
    delimit_plus = pc.Delimit_Stage('+')
    delimit_underscore = pc.Delimit_Stage('_')
    delimit_dash = pc.Delimit_Stage('-')
    delimit_slash = pc.Delimit_Stage('/')
    block_cell_line_key = pc.BlockCellLineNonCellLineKey_Stage()
    subphrase_linked = pc.RemoveSubIntervalOfMatchedBlockAncestralLink_Stage()
    #cellline_to_implied_disease = pc.CellLineToImpliedDisease_Stage()
    acr_to_expan = pc.AcronymToExpansion_Stage()
    # exact_match = pc.ExactStringMatching_Stage(["1", "2", "4", "5", "7", "8", "9"], query_len_thresh=3)
    exact_match = pc.ExactStringMatching_Stage(["1", "2", "4", "5", "7", "9", "19"], query_len_thresh=3)
    fuzzy_match = pc.FuzzyStringMatching_Stage(0.1, query_len_thresh=3)
    two_char_match = pc.TwoCharMappings_Stage()
    time_unit = pc.ParseTimeWithUnit_Stage()

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
        linked_super,
        #cellline_to_implied_disease,
        subphrase_linked,
        #cust_conseq,
        real_val,
        filt_match_priority #,
        #infer_cell_line,
        #infer_dev_stage,
        #cell_culture
    ]
    # return pc.Pipeline(stages, defaultdict(lambda: 1.0))
    return pc.Pipeline(stages, defaultdict(dd_init))


# Load ontologies
sys.stderr.write('Loading ontologies\n')
ont_name_to_ont_id = {
    "UBERON": "12",
    "CL": "1",
    "DOID": "2",
    "EFO": "16",
    "CVCL": "4",
    "ORDO": "19"}
ont_id_to_og = {x: load_ontology.load(x)[0]
                for x in list(ont_name_to_ont_id.values())}
pipeline = p_48()
dill.dump_session('pipeline_init.dill')
