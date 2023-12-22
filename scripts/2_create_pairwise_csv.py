"""
This script takes the initial passim outputs 
(json or parquet; pairwise or aggregated before),
adds a number of alignment-level statistics,
groups them by book pairs,
and stores them in pairwise csv files.

The output files will have the same structure
as the passim outputs with five additional columns:
    ch_match: the number of aligned characters
      in the alignment (not counting spaces),
    align_len: length of the aligned string
      (to be used in percentage value),
    matches_percentage: character match percentage using passim column "matches", which counts white spaces in
      char match, and 'align_len' column, which we produce above,
    w_match: word match

Usage: `1_align_stats_CM-WM.py <passim_output_folder> <pairwise_csv_folder>`

"""

from __future__ import print_function

import os
import re
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F


def word_count(s1, s2):
    """count the number of (space-delimited) words in an alignment."""
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


def match_per(match, length):
    """calculate the percentage of matched characters
    by the length of the alignment

    Example:
        > s1 = "that is a------ great-- example..."
        > s2 = "this is an even greater example!--"
        > matches = 21
        > length = 34
        > match_per(matches, length)
        61.76470588235294
    """
    
    if length == 0:
        return 0
    else:
        return (match/length) * 100


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 2:
        print("Usage: 1_align-stats_CM-WM.py <input> <output>", file=sys.stderr)
        exit(-1)

    # start the spark session:
    spark = SparkSession.builder \
        .getOrCreate()

    # define column functions to be used by pySpark:
    word_match = F.udf(lambda s1, s2: word_count(s1, s2), IntegerType())
    ch_match = F.udf(lambda s1, s2: ch_count(s1, s2), IntegerType())
    match_percent = F.udf(lambda match, align_len: match_per(match, align_len), FloatType())
    align_len = F.udf(lambda s: len(s), IntegerType())
    ch_match_percent_col = F \
        .when(F.col("align_len") == 0, 0.0) \
        .otherwise(match_percent('matches', 'align_len'))

    # check whether the input format is JSON or parquet
    # (NB: passim outputs are 
    in_folder = sys.argv[1]
    if in_folder.strip("/").endswith(".json"):
        file_type = "json"
    elif in_folder.strip("/").endswith(".parquet"):
        file_type = "parquet"
    else:
        file_type = ""
        while file_type not in ["json", "parquet"]:
            print("Did not recognise input format."):
            file_type = "Please provide input format: 'json' or 'parquet': ")
    print("fType: ", file_type)

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
        .save(sys.argv[2])
    spark.stop()

    # rename the files output by spark to "<book1>_<book2>.csv"
    # and clean up the spark output:
    for root, dirs, files in os.walk(sys.argv[2], topdown=False):
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
                # new_f_name = b1 + "_" + b2 + '.csv.gz'
                new_f_name = b1 + "_" + b2 + '.csv'
                new_f_path = os.path.join(root, new_f_name)
                # get the parent path of the root
                parent = os.path.abspath(os.path.join(root, os.pardir))
                # os.rename(os.path.join(root, rfiles[0]), re.sub('series2=', '', root) + '.json.gz')
                # rename the path of the file (root + old filename) to the path of parent dir (of root) + new filename.
                # E.g., changes /home/data/series1=JK001/series2=JK002/part.json.gz" to
                # "/home/data/series1=JK001/JK001_JK002.json.gz"
                os.rename(os.path.join(root, data_files[0]),
                          os.path.join(parent, new_f_name))
            os.rmdir(root)
        elif '/series1=' in root:
            os.rename(root, re.sub('series1=', '', root))
