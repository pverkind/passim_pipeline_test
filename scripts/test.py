"""
Insert milestone tags into OpenITI text files.
"""


import re
import os
import math
import sys
from itertools import groupby
import openiti.helper.ara as ara
from collections import Counter


def insert_milestone_tags(infp, outfp, ms_len, last_ms_cnt, log_file,
                          tok_count_function=ara.ar_tok_cnt,
                          token_regex=ara.ar_tok, ms_regex=" *ms[A-Z]?\d+",
                          meta_splitter = "\n*#META#Header#End#\n*"):
    """Insert milestone tags into a text file after every `ms_len` tokens.

    Args:
        infp (str): path to the original text file
        outfp (str): path to the output text file
        ms_len (int): maximum number of relevant tokens in a milestone
        last_ms_cnt (int): the last milestone number in an
            earlier part of the text file
            (if text file was split because it was too large)
        log_file (Obj): file object in which the errors are logged
        tok_count_function (func): the function to be used to count
            relevant tokens in the text file (e.g., only Arabic tokens)
        token_regex (str): a regular expression defining what tokens
            should count for the `ms_len`
        ms_regex (str): a regular expression defining what milestones
            in the text look like.
        meta_splitter (str):  a regular expression that defines what
            the end of the metadata header looks like
    """
    # check whether the text file is a continuation of another text file:
    file_name = re.split("-[a-z]{3}\d{1}(\.(mARkdown|inProgress|completed))?$", infp.split("/")[-1])[0]
    if re.search("[A-Z]{1}$", file_name):
        continuation = True
    else:
        continuation = False

    with open(infp, "r", encoding="utf8") as f:
        data = f.read()

        # Check whether the file contains a metadata header:
        if meta_splitter:
            try:
                if not meta_splitter.startswith("("):
                    meta_splitter = "(" + meta_splitter + ")"
                header, splitter, text = re.split(meta_splitter, data, maxsplit=1)
            except:
                log_file.write("The %s is missing the splitter! \n" % infp)
                return -1
        else:
            header = ""
            meta_splitter = ""
            text = data
        
        # remove the final new line and spaces to avoid having the milestone tag in a new empty line
        text = text.rstrip()

        mss = re.split("("+ms_regex+")", text)
        for i, m in enumerate(mss):
            if m and "ms" not in m:
                ms_toks = re.findall(token_regex, m)
                if len(ms_toks) != 300:
                    print(m)
                    print(mss[i+1], ":", len(ms_toks), "tokens")
                    for tok in ms_toks:
                        print(tok)
                    print("-------------")



def get_id(filename):
    """Get the version ID without language component and file extension,
    and without the marker for split texts"""
    # split off the language component and file extension:
    v_id = re.split("-[a-z]{3}\d{1}", filename)[0]
    # remove the final letter from version IDs
    # of books that are split into multiple files
    # (e.g., 1111Majlisi.BiharAnwar.Shia001432VolsA,
    #        1111Majlisi.BiharAnwar.Shia001432VolsB):
    if re.search("[A-Z]{1}$", v_id):
        return v_id[:-1]
    else:
        return v_id


def process_files(main_folder, output_folder, ms_len,
                  text_file_name_regex, ms_regex=" *ms[A-Z]?\d+"):
    log_f = open("insert_milestone_tags_log.txt", "a")
    
    
    # process all texts in the corpus:
    for root, dirs, files in os.walk(main_folder):
        # create a list of all text files in a book folder:
        text_files = [f for f in files if re.search(text_file_name_regex, f)]
        
        # group books by their version ID into a list of lists:
        grouped_books = [list(items) for gr, items in groupby(sorted(text_files), key=lambda filename: get_id(filename))]

        for group in grouped_books:
            print(group)
            # insert milestones normally in all files that were not split because of their size:
            if not all(re.search("[A-Z]$", re.split("-[a-z]{3}\d{1}", fn)[0]) for fn in sorted(group)):
                for fn in group:
                    infp = os.path.join(root, fn)
                    if output_folder:
                        outfp = os.path.join(output_folder, fn)
                    else:
                        outfp = infp
                    insert_milestone_tags(infp, outfp, ms_len, 0, log_f, ms_regex=ms_regex)

            # if a text file was split because of size,
            # do not restart numbering at 0 after first text file:
            elif any(re.search("[A-Z]$", re.split("-[a-z]{3}\d{1}", x)[0]) for x in sorted(group)):
                group.sort(key=lambda f: f.split("-")[1])
                grouped_extensions = [list(items) for gr, items in groupby(group,
                                                                           key=lambda name: name.split("-")[1])]
                for sub_g in grouped_extensions:
                    prev_ms_cnt = 0
                    for fn in sorted(sub_g):
                        infp = os.path.join(root, fn)
                        if output_folder:
                            outfp = os.path.join(output_folder, fn)
                        else:
                            outfp = infp
                        prev_ms_cnt = insert_milestone_tags(infp, outfp, ms_len, prev_ms_cnt, log_f, ms_regex=ms_regex)
    log_f.close()


if __name__ == '__main__':
    folder = input("Enter the path to the folder containing the original texts: ").strip()
    outfolder = input("Enter the path to the output folder for the (re)milestoned texts: ").strip()
    if outfolder == folder:
        outfolder = ""
    ms_length = input("Enter the number of tokens per milestone: ")
    text_file_name_regex="^\d{4}\w+\.\w+\.\w+-[a-z]{3}\d{1}(\.(mARkdown|inProgress|completed))?$"
    
    process_files(folder, outfolder, int(ms_length), text_file_name_regex)
    print("Done insterting milestones!")

