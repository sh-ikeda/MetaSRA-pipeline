#!/bin/bash

# TODO add the map_sra_to_ontology module to the PYTHONPATH
# Example:
#   export PYTHONPATH=<path to directory containing map_sra_to_ontology>:$PYTHONPATH
#export PYTHONPATH=/mnt/c/Users/togotv_dell1/work/biosample/MetaSRA-pipeline/:$PYTHONPATH

# TODO add the bktree module to the PYTHONPATH
# Example:
#   export PYTHONPATH=<path to directory containing bktree.py script>:$PYTHONPATH
#export PYTHONPATH=/mnt/c/Users/togotv_dell1/work/biosample/MetaSRA-pipeline/bktree/:$PYTHONPATH
METASRA_PATH=$(dirname $(pwd))
export PYTHONPATH=$METASRA_PATH/:$METASRA_PATH/bktree/:$METASRA_PATH/map_sra_to_ontology/:$PYTHONPATH

# Download ontologies
## .owl files are converted to .obo here.
echo "Downloading ontologies..."
python download_ontologies.py

# Reformat Cellosaurus
echo "Reformating Cellosaurus..."
python reformat_cellosaurus.py

# Download SPECIALIST Lexicon
echo "Downloading SPECIALIST Lexicon..."
python download_specialist_lexicon.py

# Build BK-tree for fuzzy string matching
echo "Building the BK-tree from the ontologies..."
mkdir ../map_sra_to_ontology/fuzzy_matching_index
python build_bk_tree.py -i 1,2,18,5,7,9,19 -j fuzzy_match_string_data.json -p fuzzy_match_bk_tree.pickle
mv fuzzy_match_bk_tree.pickle ../map_sra_to_ontology/fuzzy_matching_index
mv fuzzy_match_string_data.json ../map_sra_to_ontology/fuzzy_matching_index

# Link the terms between ontologies
echo "Linking ontologies..."
python link_ontologies.py
python superterm_linked_terms.py
cp term_to_superterm_linked_terms.json ../map_sra_to_ontology/metadata

# Generate cell-line to disease implications
echo "Generating cell-line to disease implications..."
python generate_implications.py
cp cellline_to_disease_implied_terms.json ../map_sra_to_ontology/metadata


