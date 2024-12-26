FROM python:3.9-bookworm

RUN pip3 install marisa-trie nltk==3.8.1 rdflib xlsxwriter
RUN python3 -c "import nltk; nltk.download('punkt')"

RUN apt-get update -y && apt-get install -y openjdk-17-jdk

WORKDIR /usr/local/bin
RUN wget https://github.com/ontodev/robot/releases/download/v1.9.5/robot.jar
RUN wget https://raw.githubusercontent.com/ontodev/robot/master/bin/robot
RUN chmod +x robot

COPY . /app/MetaSRA-pipeline/

WORKDIR /app/MetaSRA-pipeline/
ENV PYTHONPATH /app/MetaSRA-pipeline/:/app/MetaSRA-pipeline/bktree/:/app/MetaSRA-pipeline/map_sra_to_ontology/

# WORKDIR /app/MetaSRA-pipeline/setup_map_sra_to_ontology/
# RUN ./setup.sh
# WORKDIR /app/MetaSRA-pipeline/
### dill fails during automated build on Docker hub because it requires ~4GB RAM
# RUN python3 dill_pipeline_init.py

CMD ["python3", "/app/MetaSRA-pipeline/run_pipeline.py"]
