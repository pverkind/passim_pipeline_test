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
        
        # remove old milestone ids:
        if ms_regex:
            text = re.sub(ms_regex, "", text)
        else:
            text = re.sub(" *Milestone300| *ms[A-Z]?\d+", "", text)

        # check if any old milestones remain in the text:
        ms_remaining = re.findall("ms[A-Z]?\d+", text)
        ms_remaining2 = re.findall("Milestone300", text)
        all_remaining = [ms_remaining, ms_remaining2]
        if ms_regex:
            ms_remaining3 = re.findall(ms_regex, text)
            all_remaining.append(ms_remaining3)
        for rem in all_remaining: 
            if len(rem) > 1:
                log_file.write("\t\tremaining IDs in %s \n" % infp)
                log_file.write("\t\t\t", ms_remaining)
                return -1


        # count the number of relevant tokens in the text
        # (e.g., all Arabic-script tokens):
        rel_toks_count = tok_count_function(text)

        # calculate the number of digits in the last milestone
        # (to know how many zeroes to pad the milestone number with):
        n_digits = len(str(math.floor(rel_toks_count / ms_len)))

        # insert milestones:
        
        # create a list of all the tokens in the text:
        all_toks = re.findall(r"\w+|\W+", text)

        token_count = 0
        ms_count = last_ms_cnt
        new_data = []
        for i in range(0, len(all_toks)):
            # check for each token whether it is relevant for the count:
            if re.search(token_regex, all_toks[i]):
                token_count += 1
                new_data.append(all_toks[i])
            else:
                new_data.append(all_toks[i])

            # insert milestone tag when the maximum number of tokens
            # (or the end of the text) was reached:
            if token_count == ms_len or i == len(all_toks) - 1:
                ms_count += 1
                if continuation:
                    milestone = " ms" + file_name[-1] + str(ms_count).zfill(n_digits)
                else:
                    milestone = " ms" + str(ms_count).zfill(n_digits)
                new_data.append(milestone)
                token_count = 0

        ms_text = "".join(new_data)

        # check whether the text has been damaged by inserting the milestone tags:
        test = re.sub(ms_regex, "", ms_text)
        if test == text:
            ms = re.findall(ms_regex, ms_text)
            # check whether every milestone number is present only once:
            dictOfIds = dict(Counter(ms))
            # Remove elements from dictionary whose value is 1, i.e. non duplicate items
            dictOfIds = { key:value for key, value in dictOfIds.items() if value > 1}
            if len(dictOfIds) == 0:
                ms_text = header.rstrip() + "\n\n" + splitter + "\n\n" + ms_text
                with open(outfp, "w", encoding="utf8") as f9:
                    f9.write(ms_text.strip())
                return ms_count
            else:
                log_file.write("\t\tduplicate ids in %s \n" % file)
                for key, value in dictOfIds.items():
                    log_file.write('ID = %s  :: Repeated Count = %d'  % (key, value))
        else:
            log_file.write("\t\tSomething got messed up... in %s \n" % file)
            return -1



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

