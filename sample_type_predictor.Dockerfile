FROM python:2.7-buster

RUN pip install numpy scipy scikit-learn setuptools marisa-trie nltk==3.0 dill
RUN python -c "import nltk; nltk.download('punkt')"

COPY . /app/MetaSRA-pipeline/

WORKDIR /app/MetaSRA-pipeline/map_sra_to_ontology/
RUN cp ontology_graph_py2.py ontology_graph.py

WORKDIR /app/MetaSRA-pipeline/
ENV PYTHONPATH /app/MetaSRA-pipeline/:/app/MetaSRA-pipeline/bktree/:/app/MetaSRA-pipeline/map_sra_to_ontology/

CMD ["python", "/app/MetaSRA-pipeline/map_sra_to_ontology/run_sample_type_predictor.py"]
