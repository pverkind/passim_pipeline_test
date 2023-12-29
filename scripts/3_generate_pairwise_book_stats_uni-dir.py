"""Generate pairwise text reuse statistics on the book level.


Usage: python3 -m book-stats_pairwise_uni-dir <passim_input> <passim_output> <meta> <output_folder>

Arguments:
    passim_input: path to the folder containing the input documents passed to passim
    passim_output: path to the folder containing the alignments output by passim (align.json / align.parquet)
    meta: path to the metadata file
    output_folder: path to the folder where the output CSV files should be stored

The script produces one or more csv files (part-....csv) in the output folder.

Output csv columns:
    _T1: short version ID of T1 (without language component and extension)
    _T2: short version ID of T2 (without language component and extension)
    instances: number of text reuse alignments between this pair of texts
    WM2_Total: total number of words in T2 that are fully matched in any alignment with T1
    WM1_Total: total number of words in T1 that are fully matched in any alignment with T2
    CM2_Total: total number of characters in T2 that are matched in any alignment with T1
    CM1_Total: total number of characters in T1 that are matched in any alignment with T2
    ch_MATCHES_Min: minimum number of matching characters in alignments between T1 and T2
    ch_MATCHES_Max: maximum number of matching characters in alignments between T1 and T2
    ch_MATCHES_Mean: mean number of matching characters in alignments between T1 and T2
    date_B1: death date (hijri era) of the author of T1
    date_B2: death date (hijri era) of the author of T2
    author_B1: Latin-script names of the author of T1 (separated by " :: ")
    author_B2: Latin-script names of the author of T2 (separated by " :: ")
    book1: book URI of T1 (<date><author>.<title>)
    book2: book URI of T2 (<date><author>.<title>)
    tok_length_B1: number of Arabic-script tokens in T1
    tok_length_B2: number of Arabic-script tokens in T2
    ch_length_B1: number of Arabic-script characters in T1
    ch_length_B2: number of Arabic-script characters in T2
    WM_B2inB1: percentage of words in T2 that are fully matched in T1 (= WM2_Total / tok_length_B2)
    WM_B1inB2: percentage of words in T1 that are fully matched in T2 (= WM1_Total / tok_length_B1)
    CM_B2inB1: percentage of Arabic-script characters in T2 that are matched in T1 (= CM2_Total / ch_length_B2)
    CM_B1inB2: percentage of Arabic-script characters in T1 that are matched in T2 (= CM1_Total / ch_length_B1)
    chrono_B1B2: chronological order of the alignment of the pair (values: chron, anachron, sr_include (sr=self-reuse), sr_exclude, include, exclude)
    chrono_B2B1: chronological order of the alignment of the pair (values: chron, anachron, sr_include (sr=self-reuse), sr_exclude, include, exclude)

"""

from __future__ import print_function

import math
import statistics
import sys, os, re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def count_dashes(s):
    """Count the number of dashes in a given string"""
    return s.count('-')


def get_common_word_indices(s1, s2, local_tok_offset1, ms1):
    """Get the start token index in book1 of each fully matching word.
    This will be used to count the number of tokens in each book that is fully reused
    in any other book in the corpus.

    Args:
        s1 (str): text of the alignment in book1
        s2 (str): text of the alignment in book2
        local_tok_offset1 (str): token offset for the start of the alignment
            inside the current milestone in book1
        ms1 (str): milestone number in book1

    Returns:
        str (comma-separated start token offsets in book 1)
    """
    # calculate the token offset of the start of the alignment in book1:
    offset1 = ((int(ms1) - 1) * 300) + int(local_tok_offset1)
    
    cnt_local_pos1 = 0
    indices1_arr = []
    i = 0
    while i < len(s1):
        # print("i: ", i)
        prev_i = i
        while i < len(s1) and s1[i] != " ":
            i += 1
        cnt_local_pos1 += 1
        # add to the indices1_arr the start token offset in book 1
        # of each (space-delineated) word that is identical in both alignments:
        if s1[prev_i:i] == s2[prev_i:i]:
            indices1_arr.append(offset1 + cnt_local_pos1)
        i += 1

    return ','.join(str(s) for s in indices1_arr)


def get_common_char_indices(s1, s2, local_ch_offset1, total_prev_ch_offset1):
    """Get the character index in book1 of each matching character in the alignment.
    This will be used to count the number of characters in each book that is reused
    in any other book in the corpus.

    Args:
        s1 (str): text of the alignment in book1
        s2 (str): text of the alignment in book2
        local_ch_offset1 (str): character offset for the start of the alignment
            inside the current milestone in book1
        total_prev_ch_offset1 (int): total number of characters in book1
            before the start of the current milestone

    Returns:
        str (comma-separated character offsets in book 1)
    """
    # calculate the character offset of the start of the alignment in book1:
    offset1 = total_prev_ch_offset1 + int(local_ch_offset1)
    
    indices1_arr = []
    max_str_len = max([len(s1), len(s2)])  # NB: s1 should be the same length as s2
    for i in range(max_str_len):
        # TODO: for book-to-book level stats, we count spaces since at the end we divide the char match value by
        #  the length of the book, which contains spaces.
        if s1[i] == s2[i]:  # and all(s != " " for s in [s1[i], s2[i]]):
            indices1_arr.append(offset1 + i)

    return ','.join(str(s) for s in indices1_arr)


def count_different_indices(x):
    """Count the number of different indices in a given list of comma-separated indices

    Args:
        x (list): list of index numbers (comma-separated strings)

    Example:
        > x = ["1,5,8", "2,5,9"]
        > count_different_indices(x)
        5

    Returns:
        int
    """
    s = set()
    for y in x:
        for t in y.split(','):
            s.add(t)

    return len(s)


def min_indices_len(x):
    """Calculate the minimum number of (character/token) indices per alignment

    Args:
        x (list): list of index numbers (comma-separated strings)

    Example:
        > x = ["1,5,8", "2,5"]
        > min_indices_len(x)
        2

    Returns:
        int
    """
    lens = []
    for y in x:
        i = y.split(',')
        lens.append(len(i))
    return min(lens)


def max_indices_len(x):
    """Calculate the maximum number of (character/token) indices per alignment

    Args:
        x (list): list of index numbers (comma-separated strings)

    Example:
        > x = ["1,5,8", "2,5"]
        > max_indices_len(x)
        3

    Returns:
        int
    """
    lens = []
    for y in x:
        lens.append(len(y.split(',')))
    return max(lens)


def mean_indices_len(x):
    """Calculate the mean number of (character/token) indices per alignment

    Args:
        x (list): list of index numbers (comma-separated strings)

    Example:
        > x = ["1,5,8", "2,5"]
        > mean_indices_len(x)
        2.5

    Returns:
        float
    """
    lens = []
    for y in x:
        lens.append(len(y.split(',')))
    # print(len(s))
    return statistics.mean(lens)


def main(meta_fp, reuse_output_folder, reuse_input_folder, outfp):
    """Generate bookwise text reuse statistics."""
    
    # start the spark session:
    spark = SparkSession.builder.appName('Stats').getOrCreate()
    
    # define column functions to be used by pySpark:
    dashCount = F.udf(lambda s: count_dashes(s), IntegerType())
    shortID = F.udf(lambda s: s.split('-')[0])
    ms_id = F.udf(lambda s: int(re.split('\.ms[A-Z]?', s)[1]))
    collect_indices_set_len = F.udf(count_different_indices, IntegerType())
    indices_len_min = F.udf(min_indices_len, IntegerType())
    indices_len_max = F.udf(max_indices_len, IntegerType())
    indices_len_mean = F.udf(mean_indices_len, FloatType())
    word_match = F.udf(lambda s1, s2, bw1, ms1: get_common_word_indices(s1, s2, bw1, ms1), StringType())
    ch_match = F.udf(lambda s1, s2, b1, total_prev_offset: get_common_char_indices(s1, s2, b1, total_prev_offset), StringType())
    l_len = F.udf(lambda l: len(l), IntegerType())
    toks = F.udf(lambda l: l.split(" ")[0])
    converted_udf = F.udf(lambda x: x.encode("ascii", "ignore"))

    # load metadata file:
    meta = spark.read \
        .format('csv') \
        .options(header='true', sep='\t') \
        .load(meta_fp)
    
    # select specific columns of book1 from metadata file
    # in order to use it in joins:
    meta1 = meta.select(
        F.col('id').alias('T1'),
        F.col('tok_length').alias('tok_len_B1'),
        F.col('char_length').alias('ch_len_B1'),
        F.col('date').alias('date_B1'),
        F.col('author_lat').alias('author_B1'),
        F.col('book').alias('book1')
        )
    # select specific columns of book2 from metadata file
    # in order to use it in joins:
    meta2 = meta.select(
        F.col('id').alias('T2'),
        F.col('tok_length').alias('tok_len_B2'),
        F.col('char_length').alias('ch_len_B2'),
        F.col('date').alias('date_B2'),
        F.col('author_lat').alias('author_B2'),
        F.col('book').alias('book2')
        )

    # read passim inputs files
    reuse_input_files = spark.read \
        .format('json') \
        .options(encoding='UTF-8') \
        .load(reuse_input_folder)

    # create dataframes from B1 and B2 input files and add 'ch_len' col
    # to save character len of each milestone text
    ms_ch_len1 = reuse_input_files\
        .withColumn('ch_len1', F.length('text')) \
        .select(
            F.col('id').alias('ser_ms1'),
            F.col('series').alias('series_b1'),
            F.col('seq').alias('seq_1'),
            F.col('ch_len1').alias('ch_len1')
            #, F.col('ms1').alias('ms1')
            )

    ms_ch_len2 = reuse_input_files\
        .withColumn('ch_len2', F.length('text')) \
        .select(
            F.col('id').alias('ser_ms2'),
            F.col('series').alias('series_b2'),
            F.col('seq').alias('seq_2'),
            F.col('ch_len2').alias('ch_len2')
            #, F.col('ms2').alias('ms2')
            )

    # define windows for each book to use
    # in accumulation of previous milestones char length
    # in each current milestone ch_len
    w1 = Window.partitionBy(ms_ch_len1["series_b1"]) \
         .orderBy(ms_ch_len1["seq_1"]) \
         .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    w2 = Window.partitionBy(ms_ch_len2["series_b2"]) \
         .orderBy(ms_ch_len2["seq_2"]) \
         .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # create dataframes that hold the sum of char length
    # of all previous milestones for each current ms in B1 and B2
    # and drop the columns that we don't need
    ms_ch_len_sum1 = ms_ch_len1 \
        .withColumn("total_prev_ch_offset1", F.sum("ch_len1").over(w1)) \
        .drop("ch_len1")

    ms_ch_len_sum2 = ms_ch_len2 \
        .withColumn("total_prev_ch_offset2", F.sum("ch_len2").over(w2)) \
        .drop("ch_len1")#, "seq2", "series_b2")

    # define the file format of the passim output files
    # based on the name of the output folder:
    if reuse_output_folder.strip("/").endswith(".json"):
        file_type = "json"
    elif reuse_output_folder.strip("/").endswith(".parquet"):
        file_type = "parquet"
    elif reuse_output_folder.strip("/").endswith(".csv"):
        file_type = "csv"
   
    # load the passim output data into spark:
    reuse_outputs_df = spark.read \
        .format(file_type) \
        .options(encoding='UTF-8') \
        .load(reuse_output_folder) \
        .distinct()

    # drop corrupt records and rows that have null values
    # in any column from passim outputs:
    reuse_outputs_df = reuse_outputs_df.drop('_corrupt_record')
    reuse_outputs_df = reuse_outputs_df.na.drop()

    
    # add a column with short version ID (without language + extension)
    # for book1 and book2:
    df1 = reuse_outputs_df \
        .withColumn('_T1', shortID('series1')) \
        .withColumn('_T2', shortID('series2'))

    # Add metadata columns for book1 and book2,
    # add the character lenght of the entire milestone,
    # and aggregate statistics for all alignments between two books:
    df1 = df1 \
        .join(meta1, df1["_T1"] == meta1["T1"], how='left') \
        .join(meta2, df1["_T2"] == meta2["T2"], how='left') \
        .join(ms_ch_len_sum1, (reuse_outputs_df["seq1"] == ms_ch_len_sum1["seq_1"]) & \
              (shortID('series1') == shortID(ms_ch_len_sum1["series_b1"]))) \
        .join(ms_ch_len_sum2, (reuse_outputs_df["seq2"] == ms_ch_len_sum2["seq_2"]) & \
              (shortID('series2') == shortID(ms_ch_len_sum2["series_b2"]))) \
        .withColumn('s1', F.regexp_replace('s1', '\n', ' ')) \
        .withColumn('s2', F.regexp_replace('s2', '\n', ' ')) \
        .withColumn('w_match1', word_match('s1', 's2', 'bw1', 'seq1')) \
        .withColumn('w_match2', word_match('s2', 's1', 'bw2', 'seq2')) \
        .withColumn('ch_match1', ch_match('s1', 's2', 'b1', 'total_prev_ch_offset1')) \
        .withColumn('ch_match2', ch_match('s2', 's1', 'b2', 'total_prev_ch_offset2')) \
        .groupBy('_T1', '_T2') \
        .agg(
            F.count('matches').alias('instances'),
            collect_indices_set_len(F.collect_set('w_match2')).alias('WM2_Total'),
            collect_indices_set_len(F.collect_set('w_match1')).alias('WM1_Total'),
            collect_indices_set_len(F.collect_set('ch_match2')).alias('CM2_Total'),
            collect_indices_set_len(F.collect_set('ch_match1')).alias('CM1_Total'),
            F.min('matches').alias('ch_MATCHES_Min'),
            F.max('matches').alias('ch_MATCHES_Max'),
            F.mean('matches').alias('ch_MATCHES_Mean'),
            F.first("date_B1").alias("date_B1"), F.first("date_B2").alias("date_B2"),
            F.first("author_B1").alias("author_B1"), F.first("author_B2").alias("author_B2"),
            F.first("book1").alias("book1"), F.first("book2").alias("book2"),
            F.first('tok_len_B1').alias('tok_length_B1'),
            F.first('tok_len_B2').alias('tok_length_B2'),
            F.first('ch_len_B1').alias('ch_length_B1'),
            F.first('ch_len_B2').alias('ch_length_B2')
            ) \
        .withColumn(
            'WM_B2inB1',
            F.col('WM2_Total') / F.col('tok_length_B2')
            ) \
        .withColumn(
            'WM_B1inB2',
            F.col('WM1_Total') / F.col('tok_length_B1')
            ) \
        .withColumn(
            'CM_B2inB1',
            F.col('CM2_Total') / F.col('ch_length_B2')
            ) \
        .withColumn(
            'CM_B1inB2',
            F.col('CM1_Total') / F.col('ch_length_B1')
            ) \
        .withColumn(
            'chrono_B1B2',
            F.when(
                F.col('date_B1').cast("integer") > F.col('date_B2').cast("integer"),
                "chron"
            ).when(
                F.col('date_B1').cast("integer") < F.col('date_B2').cast("integer"),
                "anachron"
            ).when(
                F.col('date_B1').cast("integer") == F.col('date_B2').cast("integer"),
                F.when(
                    F.col('author_B1') == F.col('author_B2'),
                    F.when(
                        F.col("book1") < F.col("book2"),
                        "sr_include"
                    ).otherwise("sr_exclude")
                ).otherwise(
                    F.when(
                        F.col("author_B1") < F.col("author_B2"),
                        "include"
                    ).otherwise("exclude")
                )
            )) \
        .withColumn(
            'chrono_B2B1',
            F.when(
                F.col('date_B2').cast("integer") > F.col('date_B1').cast("integer"),
                "chron"
                )
            .when(
                F.col('date_B2').cast("integer") < F.col('date_B1').cast("integer"),
                "anachron"
                )
            .when(
                F.col('date_B1').cast("integer") == F.col('date_B2').cast("integer"),
                F.when(
                    F.col('author_B1') == F.col('author_B2'),
                    F.when(
                        F.col("book2") < F.col("book1"),
                        "sr_include"
                        ).otherwise("sr_exclude")
                ).otherwise(
                    F.when(
                        F.col("author_B2") < F.col("author_B1"),
                        "include"
                        ).otherwise("exclude")
                    )
                )) \
        .repartition('_T1') \
        .sortWithinPartitions('_T2') \
        .coalesce(1) \
        .write \
        .format('csv') \
        .options(header='true', sep='\t', compression='gzip') \
        .mode('overwrite') \
        .save(outfp)

    spark.stop()
  

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python3 -m book-stats_pairwise_uni-dir <passim_input_folder> <passim_output_folder> <meta_file> <output_folder>", file=sys.stderr)
        exit(-1)

    reuse_input_folder = sys.argv[1]
    reuse_output_folder = sys.argv[2]
    meta_fp = sys.argv[3]
    outfp = sys.argv[4]
    main(meta_fp, reuse_output_folder, reuse_input_folder, outfp)


