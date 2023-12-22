"""
This script takes the initial passim outputs 
(json or parquet; pairwise or aggregated before),
adds a number of alignment-level statistics,
groups them by book pairs,
and stores them in pairwise csv files.

Usage: `python3 -m 2_create_pairwise_csv.py <passim_output_folder> <pairwise_csv_folder> --bidirectional`

The output files will have the same structure
as the passim outputs with five additional columns:
    ch_match: the number of aligned characters
      in the alignment (not counting spaces),
    align_len: length of the aligned string
      (to be used in percentage value),
    matches_percentage: character match percentage using passim column "matches", which counts white spaces in
      char match, and 'align_len' column, which we produce above,
    w_match: word match


NB: to save computational resources, we generate text reuse data uni-directionally:
a pair of documents is compared only once (document1 to document2, not document2 to document1).
This is achieved by passing the following parameter to passim: 
`--filterpairs 'gid < gid2'` (in which gid is a unique numeric ID for the first document in a pair,
and gid2 a unique numeric ID for the second document in a pair): 
documents from a book should be compared only to documents from books with a higher number.

Imagine that we had three texts, with text reuse detected between all three of them,
the script would generate this folder structure:

```
book1
    |- book1_book2.csv
    |- book1_book3.csv
book2
    |-book2_book3.csv
```

In order to make it easier to work with the pairwise csv files, we can create a copy of
each of these pairwise book files (`<book1ID>_<book2ID>.csv`) in the other direction
(`<book2ID>_<book1ID>.csv`).
The result will be that for each book for which passim found text reuse,
there will be a folder that contains a csv file for each book passim found text reuse with: 

```
book1
    |- book1_book2.csv
    |- book1_book3.csv
book2
    |- book2_book1.csv
    |- book2_book3.csv
book3
    |- book3_book1.csv
    |- book3_book2.csv
```

To get this "bi-directional" folder structure, either add the flag "--bidirectional"
when running the script; or reply "Y" when prompted by the script.
If, for reasons of size, you prefer the uni-directional structure, reply "N".



"""

from __future__ import print_function

import csv
import os
import re
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F


def word_count(s1, s2):
    """count the number of exactly matched (space-delimited)
    words in an alignment.
    
    Example:
        > s1 = "that is a------ great-- example..."
        > s2 = "this is an even greater example!--"
        > word_count(s1, s2)
        1

    (NB: only the word "is" fulfills the criteria!)
    """
    cnt = 0
    i = 0
    while i < len(s1):
        # print("i: ", i)
        prev_i = i
        while i < len(s1) and s1[i] != " ":
            i += 1
        if s1[prev_i:i] == s2[prev_i:i]:
            cnt += 1
        i += 1
    return cnt


def ch_count(s1, s2, exclude_spaces=True):
    """Count the number of aligned characters in an alignment

    NB: alignments are always the same length.

    Example:
        > s1 = "that is a------ great-- example..."
        > s2 = "this is an even greater example!--"
        > ch_count(s1, s2, exclude_spaces=True)
        17
        > ch_count(s1, s2, exclude_spaces=False)
        21
    """
    cnt = 0
    for i in range(0, len(s1)):
        #if s1[i] == s2[i] and all(s != " " for s in [s1[i], s2[i]]):
        if s1[i] == s2[i]:
            if not exclude_spaces or (exclude_spaces and s1[i] != " "):
                cnt += 1
    return cnt


def match_pct(match, length):
    """calculate the percentage of matched characters
    by the length of the alignment

    Example:
        > s1 = "that is a------ great-- example..."
        > s2 = "this is an even greater example!--"
        > matches = 21
        > length = 34
        > match_pct(matches, length)
        61.76470588235294
    """
    
    if length == 0:
        return 0
    else:
        return (match/length) * 100

def create_bi_dir(uni_dir_path):
    """Create a copy of each csv file, in the other direction:
    b1/b1_b2.csv => b2/b2_b1.csv

    Args:
        uni_dir_path (str): path to the folder containing the
            uni-directional csv files.
    """
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


def main(in_folder, out_folder, file_type, bi_dir):
    """Generate csv files from passim output files

    Args:
        in_folder (str): path to the input folder
            (align.json or align.parquet folder in the passim output folder)
        out_folder (str): path to the output folder
        file_type (str): "json" or "parquet"
        bi_dir (bool): if True, duplicate pairwise files for bi-directional use.
    """
    # start the spark session:
    spark = SparkSession.builder \
        .getOrCreate()

    # define column functions to be used by pySpark:
    word_match = F.udf(lambda s1, s2: word_count(s1, s2), IntegerType())
    ch_match = F.udf(lambda s1, s2: ch_count(s1, s2), IntegerType())
    match_percent = F.udf(lambda match, align_len: match_pct(match, align_len), FloatType())
    align_len = F.udf(lambda s: len(s), IntegerType())
    ch_match_percent_col = F \
        .when(F.col("align_len") == 0, 0.0) \
        .otherwise(match_percent('matches', 'align_len'))

    # load the records:
    df = spark.read \
         .format(file_type) \
         .options(encoding='UTF-8') \
         .load(in_folder) \
         .distinct()
    # drop records that passim identified as corrupt records:
    df = df.drop('_corrupt_record')
    # drop rows that have null values in any column from passim outputs:
    df = df.na.drop()

    # add alignment stats, group by book pairs,
    # and store as temp csv files
    # (directory structure: series1=xxx/series2=yyy/part-zzz):
    dfGrouped = df \
        .withColumn('ch_match', ch_match('s1', 's2')) \
        .withColumn('align_len', align_len('s1')) \
        .withColumn('matches_percent', ch_match_percent_col) \
        .withColumn('w_match', word_match('s1', 's2')) \
        .withColumn('series_b1', F.col('series1')) \
        .withColumn('series_b2', F.col('series2')) \
        .repartition('series1', 'series2') \
        .sortWithinPartitions('id1', 'id2') \
        .write \
        .partitionBy('series1', 'series2') \
        .format('csv') \
        .options(header='true', delimiter='\t') \
        .mode('overwrite') \
        .save(out_folder)
    spark.stop()

    # rename the files output by spark to "<book1>_<book2>.csv"
    # and clean up the spark output:
    for root, dirs, files in os.walk(out_folder, topdown=False):
        if '/series2=' in root:
            data_files = list(filter(lambda s: not s.startswith('.'), files))
            dot_files = list(filter(lambda s: s.startswith('.'), files))
            for dot_file in dot_files:
                os.remove(os.path.join(root, dot_file))
            if (len(data_files) > 0) and (data_files[0].startswith('part')):
                tmp1 = root.split("/series1=")[1]
                tmp2 = tmp1.split("/series2=")
                b1 = tmp2[0]
                b2 = tmp2[1]
                # generate new filename and join it to the root path
                new_f_name = b1 + "_" + b2 + '.csv'
                new_f_path = os.path.join(root, new_f_name)
                # get the parent path of the root
                parent = os.path.abspath(os.path.join(root, os.pardir))
                # rename the path of the file (root + old filename) to the path of parent dir (of root) + new filename.
                # E.g., changes /home/data/series1=JK001/series2=JK002/part.json.gz" to
                # "/home/data/series1=JK001/JK001_JK002.json.gz"
                os.rename(os.path.join(root, data_files[0]),
                          os.path.join(parent, new_f_name))
            os.rmdir(root)
        elif '/series1=' in root:
            os.rename(root, re.sub('series1=', '', root))

    # generate bi-directional data:
    if bi_dir:
        create_bi_dir(out_folder)


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) > 1 and sys.argv[1] in "--help":
        print("""
Generate pairwise csv files from passim's json or parquet output.

Usage:
    python3 -m 2_create_pairwise_csv <passim_output_alignment_folder> <pairwise_csv_folder> (--bidirectional)

Parameters:

    passim_output_alignment_folder: the align.json or align.parquet
    folder in passim's output folder
    
    pairwise_csv_folder: the output folder for the generated csv files

    --bidirectional: if this flag is added, each book folder will
    contain a csv file for each other book with which it shares reuse.

    --unidirectional: if this flag is added, each book pair will
    be documented by only one csv file (b1_b2.csv OR b2_b1.csv, not both).
""", file=sys.stderr)
        exit(-1)

    # get the input folder from the command line arguments or user input:
    try:
        in_folder = sys.argv[1]
    except:
        print("Provide the path to the align.json or align.parquet folder")
        in_folder = input("inside the passim output folder: ")

    # check whether the input format is json or parquet    
    if in_folder.strip("/").endswith(".json"):
        file_type = "json"
    elif in_folder.strip("/").endswith(".parquet"):
        file_type = "parquet"
    else:
        file_type = ""
        while file_type not in ["json", "parquet"]:
            print("Did not recognise input format.")
            file_type = input("Please provide input format: 'json' or 'parquet': ")
    print("fType: ", file_type)

    # get the output folder from the command line arguments or user input:
    try:
        out_folder = sys.argv[2]
    except:
        print("Provide the path to the folder where you want")
        out_folder = input("to store the csv files: ")

    # Check whether the output should be generated in bi-directional
    # or uni-directional format:
    bi_dir = None
    if len(sys.argv) > 1:
        if "bidirectional" in sys.argv[-1]:
            bi_dir = True
        elif "unidirectional" in sys.argv[-1]:
            bi_dir = False
    if bi_dir is None:
        print("Do you want to create copies of the uni-directional csv files")
        r = input("so that each folder contains a csv for all related books? Y/n: ")
        if r.strip().upper() == "Y":
            bi_dir = True

    main(in_folder, out_folder, file_type, bi_dir)
    

    

            
    
    
