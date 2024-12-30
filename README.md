# MetaSRA: normalized human sample-specific metadata for the Sequence Read Archive

This repository contains the code implementing the pipeline used to construct the MetaSRA database described in the publication: https://doi.org/10.1093/bioinformatics/btx334

This pipeline re-annotates key-value descriptions of biological samples using biomedical ontologies.

The MetaSRA can be searched and downloaded from: http://metasra.biostat.wisc.edu/

## Usage of the Docker version
```sh
docker pull shikeda/metasra:latest
docker run -v `pwd`:/data --rm shikeda/metasra:latest -c -d -f /data/input.json -i config/pipeline_init.pickle -o /data/output.tsv
```
- `-f /data/input.json`  
  Specify the input file path. The input json file is assumed to be formatted as the format of json provided by the API of EBI BioSamples. [example](https://www.ebi.ac.uk/biosamples/samples/SAMN13719297.json)
- `-i config/pipeline_init.pickle`  
  Specify the pipeline configuration file. For standard usage, use `config/pipeline_init.pickle`.
- `-o /data/output.tsv`  
  Specify the output file name.

## Usage in local environment

### Dependencies

This project requires the following Python libraries:
- setuptools (https://pypi.python.org/pypi/setuptools)
- marisa-trie (https://pypi.python.org/pypi/marisa-trie)

### Setup

In order to run the pipeline, a few external resources must be downloaded and configured.  First, set up the PYTHONPATH environment variable to point to the directory containing the map_sra_to_ontology directory as well as to the bktree directory.  Then, to set up the pipeline, run the following commands:

    cd ./setup_map_sra_to_ontology
    ./setup.sh

This script will download the latest ontology OBO files, the SPECIALIST Lexicon files, and configure the ontologies to work with the pipeline.

### Usage

The pipeline can be run on a set of sample-specific key-value pairs
using the run_pipeline.py script. This script is used as follows:

    python run_pipeline.py -f <input key-value pairs JSON file> -i config/pipeline_init.pickle -o <output file name>

The script accepts as input a JSON file storing an array of objects which have corresponding accession id of BioSample and its characteristics (attributes) key-value pairs. Input example is at `analysis_data/input_example/test.json`. The JSON file can be retrieved from the EBI BioSamples (download JSON from the page of each entry, then put the content in an array).
