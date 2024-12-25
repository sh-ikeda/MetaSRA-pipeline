import pickle
import sys

with open(sys.argv[1], "rb") as f:
    og = pickle.load(f)[0]
print(og.id_to_term[sys.argv[2]])
