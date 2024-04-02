from optparse import OptionParser
import datetime
import subprocess
import json
import os
from os.path import join


def main():
    parser = OptionParser()
    (options, args) = parser.parse_args()

    obo_rel_loc = "../map_sra_to_ontology/obo"

    prefix_to_filename = {}
    date_str = datetime.datetime.now().strftime("%y-%m-%d")
    with open("ontology_name_to_url.json", "r") as f:
        for ont_prefix, url in json.load(f).items():
            file_ext = os.path.splitext(url)[1]
            download_file_name = join(obo_rel_loc, f"{ont_prefix}.{date_str}{file_ext}")
            obo_file_name = f"{ont_prefix}.{date_str}.obo"
            # obo_f_name = join(obo_rel_loc, file_name)
            output_f = open(download_file_name, "w")
            subprocess.call(["curl", url], stdout=output_f)
            prefix_to_filename[ont_prefix] = obo_file_name
            if file_ext == ".owl":
                subprocess.call(["robot", "convert", "--input",
                                 download_file_name,
                                 "--output",
                                 obo_file_name])

    with open("../map_sra_to_ontology/ont_prefix_to_filename.json", "w") as f:
        f.write(json.dumps(prefix_to_filename, indent=4, separators=(",", ": ")))


if __name__ == "__main__":
    main()
