"""
Generate bi-directional text reuse data files from uni-directional csv files.

We generate text reuse data uni-directionally:
a pair of documents is compared in only one direction
(this is achieved by passing this parameter to passim
when running passim: `--filterpairs 'gid < gid2'`,
in which gid is a unique numeric ID for the first book in a pair,
and gid2 a unique numeric ID for the second book in that pair). 

In a first post-processing step, we grouped all alignments
in passim's output by the pair of books to which they belong:
one csv file per book pair. The csv files follow this naming
convention: `<book1ID>_<book2ID>.csv`
(e.g., "Shamela000001.mARkdown_Shamela000002.csv").

This module creates a copy of each pairwise file `<book1ID>_<book2ID>.csv`
and stores it in the other direction: `<book2ID>_<book1ID>.csv`
"""

import os
import csv
import re

if __name__ == '__main__':
    uni_dir_path = input("Enter the path to the uni-dirctional csv data: ")

    headers = ["align_len", "b1", "b2", "bw1", "bw2", "ch_match", "e1", "e2", "ew1", "ew2", "gid1", "gid2",
               "id1", "id2", "len1", "len2", "matches", "matches_percent", "s1", "s2", "score", "seq1", "seq2",
               "series_b1", "series_b2", "tok1", "tok2", "uid1", "uid2", "w_match", "first1", "first2"]
    for root, dirs, files in os.walk(uni_dir_path):
        
        # only process the pairwise csv files, not any other files, in the uni_dir_path folder:
        rfiles = filter(lambda s: not s.startswith(('.', '_')), files)
        
        for file in rfiles:
            # path before the book1 dir, e.g., /home/admin/data/
            root_dir = root.rsplit("/", 1)[0]
            print("root: ", root)
            print("file: ", file)

            # get both book IDs from the csv filename:
            file_name_parts = file.split("_")
            b1 = file_name_parts[0]
            b2 = re.sub(".csv(.gz)?", "", file_name_parts[1])  # remove the extension
            print("b1: ", b1)
            print("b2: ", b2)
            
            # generate new file name and path for the other direction of data:
            new_file_name = b2 + "_" + b1 + ".csv"
            new_file_dir = os.path.join(root_dir, b2)
            print("new: ", new_file_dir)
            
            # create the new dir and file for the other direction of data
            os.makedirs(new_file_dir, exist_ok=True)

            # flip the direction and store the new file:
            with open(os.path.join(root, file), "r", encoding='utf8') as uni_dir_file:
                uni_dir_data = csv.DictReader(uni_dir_file, delimiter='\t')
                dh = dict((h, h) for h in uni_dir_data.fieldnames)
                with open(os.path.join(new_file_dir, new_file_name), "w") as new_file:
                    writer = csv.DictWriter(new_file, fieldnames=uni_dir_data.fieldnames, delimiter='\t')
                    writer.writeheader()
                    for u_data in uni_dir_data:
                        writer.writerow({
                            "align_len": u_data["align_len"],
                            "b1": u_data["b2"],
                            "b2": u_data["b1"],
                            "bw1": u_data["bw2"],
                            "bw2": u_data["bw1"],
                            "ch_match": u_data["ch_match"],
                            "e1": u_data["e2"],
                            "e2": u_data["e1"],
                            "ew1": u_data["ew2"],
                            "ew2": u_data["ew1"],
                            "gid1": u_data["gid2"],
                            "gid2": u_data["gid1"],
                            "id1": u_data["id2"],
                            "id2": u_data["id1"],
                            "len1": u_data["len2"],
                            "len2": u_data["len1"],
                            "matches": u_data["matches"],
                            "matches_percent": u_data["matches_percent"],
                            "s1": u_data["s2"],
                            "s2": u_data["s1"],
                            "score": u_data["score"],
                            "seq1": u_data["seq2"],
                            "seq2": u_data["seq1"],
                            "series_b1": u_data["series_b2"],
                            "series_b2": u_data["series_b1"],
                            "tok1": u_data["tok2"],
                            "tok2": u_data["tok1"],
                            "uid1": u_data["uid2"],
                            "uid2": u_data["uid1"],
                            "w_match": u_data["w_match"],
                            "first1": u_data["first2"],
                            "first2": u_data["first1"]
                        })
