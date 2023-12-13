import dill
import sys
from os.path import join
import pkg_resources as pr
import json
import datetime
from map_sra_to_ontology.predict_sample_type.learn_classifier import *

# The dilled objects need the python path to point to the predict_sample_type
# directory
sys.path.append(pr.resource_filename(__name__, "predict_sample_type"))


def run_sample_type_prediction(tag_to_val, mapped_terms, real_props, vectorizer, model):
    # Make sample-type prediction
    feat_v = vectorizer.convert_to_features(
        get_ngrams_from_tag_to_val(tag_to_val), mapped_terms
    )
    predicted, confidence = model.predict(feat_v, mapped_terms, real_props)

    return predicted, confidence


def main():
    input_f = sys.argv[-1]
    with open(input_f, "r") as f:
        input_json = json.load(f)

    ct = datetime.datetime.now()
    sys.stderr.write("[{}] Loading dilled vectorizer and model\n".format(ct))
    # Load the dilled vectorizer and model
    vectorizer_f = pr.resource_filename(
        __name__, join("predict_sample_type", "sample_type_vectorizor.dill")
    )
    classifier_f = pr.resource_filename(
        __name__, join("predict_sample_type", "sample_type_classifier.dill")
    )
    with open(vectorizer_f, "rb") as f:
        vectorizer = dill.load(f)
    with open(classifier_f, "rb") as f:
        model = dill.load(f)
    ct = datetime.datetime.now()
    sys.stderr.write("[{}] Start prediction\n".format(ct))
    l = float(len(input_json))
    i = 1.0
    j = 1.0
    for s in input_json:
        predicted, confidence = run_sample_type_prediction(
            s["tag_to_val"], s["mapped_terms"], s["real_val_props"], vectorizer, model
        )
        print("\t".join([s["tag_to_val"]["accession"], predicted, str(confidence)]))
        if i / l > j / 10:
            ct = datetime.datetime.now()
            sys.stderr.write("[{}] {}/{}\n".format(ct, int(i), int(l)))
            j += 1.0
        i += 1.0


if __name__ == "__main__":
    main()
