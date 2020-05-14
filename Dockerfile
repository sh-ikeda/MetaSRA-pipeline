FROM python:3.6-buster

RUN pip3 install numpy scipy scikit-learn setuptools marisa-trie nltk dill
RUN python3 -c "import nltk; nltk.download('punkt')"

COPY . /app/MetaSRA-pipeline/

WORKDIR /app/MetaSRA-pipeline/
ENV PYTHONPATH /app/MetaSRA-pipeline/:/app/MetaSRA-pipeline/bktree/:/app/MetaSRA-pipeline/map_sra_to_ontology/

# WORKDIR /app/MetaSRA-pipeline/setup_map_sra_to_ontology/
# RUN ./setup.sh
# WORKDIR /app/MetaSRA-pipeline/
### dill fails during automated build on Docker hub because it requires ~4GB RAM
# RUN python3 dill_pipeline_init.py

CMD ["python3", "/app/MetaSRA-pipeline/run_pipeline.py"]
