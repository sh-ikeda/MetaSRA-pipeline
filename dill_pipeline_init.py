
import sys
import dill
from collections import defaultdict

import map_sra_to_ontology
from map_sra_to_ontology import ontology_graph
from map_sra_to_ontology import load_ontology
from map_sra_to_ontology import config
from map_sra_to_ontology import pipeline_components as pc
import datetime


def p_48(ont_id_to_og):
    sys.stderr.write('[{}] spec_lex\n'.format(datetime.datetime.now()))
    spec_lex = pc.SpecialistLexicon(config.specialist_lex_location())
    sys.stderr.write('[{}] inflec_var\n'.format(datetime.datetime.now()))
    inflec_var = pc.SPECIALISTLexInflectionalVariants(spec_lex)
    sys.stderr.write('[{}] spell_var\n'.format(datetime.datetime.now()))
    spell_var = pc.SPECIALISTSpellingVariants(spec_lex)
    sys.stderr.write('[{}] key_val_filt\n'.format(datetime.datetime.now()))
    key_val_filt = pc.KeyValueFilter_Stage()
    sys.stderr.write('[{}] init_tokens_stage\n'.format(datetime.datetime.now()))
    init_tokens_stage = pc.InitKeyValueTokens_Stage()
    sys.stderr.write('[{}] ngram\n'.format(datetime.datetime.now()))
    ngram = pc.NGram_Stage()
    sys.stderr.write('[{}] lower_stage\n'.format(datetime.datetime.now()))
    lower_stage = pc.Lowercase_Stage()
    sys.stderr.write('[{}] man_at_syn\n'.format(datetime.datetime.now()))
    man_at_syn = pc.ManuallyAnnotatedSynonyms_Stage()
    #infer_cell_line = pc.InferCellLineTerms_Stage()
    sys.stderr.write('[{}] prop_spec_syn\n'.format(datetime.datetime.now()))
    prop_spec_syn = pc.PropertySpecificSynonym_Stage()
    #infer_dev_stage = pc.ImpliedDevelopmentalStageFromAge_Stage()
    sys.stderr.write('[{}] linked_super\n'.format(datetime.datetime.now()))
    linked_super = pc.LinkedTermsOfSuperterms_Stage()
    #cell_culture = pc.ConsequentCulturedCell_Stage()
    sys.stderr.write('[{}] filt_match_priority\n'.format(datetime.datetime.now()))
    filt_match_priority = pc.FilterOntologyMatchesByPriority_Stage()
    sys.stderr.write('[{}] real_val\n'.format(datetime.datetime.now()))
    real_val = pc.ExtractRealValue_Stage()
    sys.stderr.write('[{}] match_cust_targs\n'.format(datetime.datetime.now()))
    match_cust_targs = pc.ExactMatchCustomTargets_Stage()
    #cust_conseq = pc.CustomConsequentTerms_Stage()
    sys.stderr.write('[{}] delimit\n'.format(datetime.datetime.now()))
    delimit_plus = pc.Delimit_Stage('+')
    delimit_underscore = pc.Delimit_Stage('_')
    delimit_dash = pc.Delimit_Stage('-')
    delimit_slash = pc.Delimit_Stage('/')
    sys.stderr.write('[{}] block_cell_line_key\n'.format(datetime.datetime.now()))
    block_cell_line_key = pc.BlockCellLineNonCellLineKey_Stage(ont_id_to_og["4"])
    sys.stderr.write('[{}] subphrase_linked\n'.format(datetime.datetime.now()))
    subphrase_linked = pc.RemoveSubIntervalOfMatchedBlockAncestralLink_Stage()
    #cellline_to_implied_disease = pc.CellLineToImpliedDisease_Stage()
    sys.stderr.write('[{}] acr_to_expan\n'.format(datetime.datetime.now()))
    acr_to_expan = pc.AcronymToExpansion_Stage()
    # exact_match = pc.ExactStringMatching_Stage(["1", "2", "4", "5", "7", "8", "9"], query_len_thresh=3)
    sys.stderr.write('[{}] exact_match\n'.format(datetime.datetime.now()))
    target_og_ids = ["1", "2", "4", "5", "7", "9", "19"]
    target_ogs = [ont_id_to_og[id] for id in target_og_ids]
    exact_match = pc.ExactStringMatching_Stage(target_ogs, query_len_thresh=3)
    sys.stderr.write('[{}] fuzzy_match\n'.format(datetime.datetime.now()))
    fuzzy_match = pc.FuzzyStringMatching_Stage(0.1, query_len_thresh=3)
    sys.stderr.write('[{}] two_char_match\n'.format(datetime.datetime.now()))
    two_char_match = pc.TwoCharMappings_Stage()
    sys.stderr.write('[{}] time_unit\n'.format(datetime.datetime.now()))
    time_unit = pc.ParseTimeWithUnit_Stage()
    sys.stderr.write('[{}] prior_spec_match\n'.format(datetime.datetime.now()))
    prior_spec_match = pc.PrioritizeSpecificMatching_Stage("", [])
    sys.stderr.write('[{}] taxid_filter\n'.format(datetime.datetime.now()))
    taxid_filter = pc.FilterMappingsToCellLinesByTaxId_Stage(ont_id_to_og["4"])
    sys.stderr.write('[{}] filter_ambiguous\n'.format(datetime.datetime.now()))
    filter_ambiguous = pc.FilterMappingsFromAmbiguousAttributes_Stage()

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
        taxid_filter,
        linked_super,
        #cellline_to_implied_disease,
        subphrase_linked,
        #cust_conseq,
        real_val,
        filt_match_priority,
        prior_spec_match
        #infer_cell_line,
        #infer_dev_stage,
        #cell_culture
    ]
    # return pc.Pipeline(stages, defaultdict(lambda: 1.0))
    return pc.Pipeline(stages)


# Load ontologies
sys.stderr.write('[{}] Loading ontologies\n'.format(datetime.datetime.now()))
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
# ont_id_to_og = {x: load_ontology.load(x)[0]
#                 for x in list(ont_name_to_ont_id.values())}
ont_id_to_og = {}
for ont_name in list(ont_name_to_ont_id.keys()):
    sys.stderr.write('[{}] {}\n'.format(datetime.datetime.now(), ont_name))
    id = ont_name_to_ont_id[ont_name]
    ont_id_to_og[id] = load_ontology.load(id)[0]
pipeline = p_48(ont_id_to_og)

del ont_name_to_ont_id["UBERON_all"], ont_name_to_ont_id["UO"], ont_name_to_ont_id["EFO_all"]
del ont_id_to_og["5"], ont_id_to_og["7"], ont_id_to_og["9"]

sys.stderr.write('[{}] dill dump\n'.format(datetime.datetime.now()))
#dill.dump_session('pipeline_init.dill')
with open("pipeline_init.dill", "wb") as f:
    dill.dump((pipeline, ont_id_to_og), f)

sys.stderr.write('[{}] Done.\n'.format(datetime.datetime.now()))
