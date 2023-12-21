"""
Prepares OpenITI corpus text files for passim input.

The script will prompt the user whether you want to process
all versions or primary versions only.

As primary version, the version marked as "pri" in the metadata is used.

If multiple text files with the same version ID but a different extension
are present, the script chooses the file according to the following priority:
mARkdown > completed > inProgress > no extension
"""

import csv
import os
import re
import sys
from itertools import groupby

import pandas as pd
from openiti.helper import funcs  # installation: `pip install openiti`


def text_to_json(book_uri, file_id, body_text, output_folder,
                 ms_regex="ms[A-Z]?\d+", threshold=1000,
                 overwrite=False, include_book_uri=False):
    """Generate linejson files containing the passim input documents.

    Args:
        book_uri (str): OpenITI book URI (author + title components)
        file_id (str): ID of the text file
          (file name minus author and title components)
        body_text (str): text without metadata header
        output_folder (str): path of the folder for output files
        ms_regex (str): milestone ID (ms001, ms002, ...)
        threshold (int): maximum number of records to write to one file
        overwrite (bool): if False, existing output files will not be overwritten
    """
    print("include_book_uri:", include_book_uri)

    target_path = os.path.join(output_folder, file_id)
    print("target : ", file_id)

    # only overwrite existing output files if explicitly told to:
    if os.path.exists(target_path + "-%05d" % thresh) and not overwrite:
        return 0

    # create a record template for each milestone document:
    rec_template = '{"id":"%s", "series":"%s", "text": "%s", "seq": %d}'
    if include_book_uri:
        rec_template = rec_template[:-1] + ', "book_uri": "' + book_uri + '"}'

    # Generate the linejson files: 
    records = []
    counter = 0
    i = 0
    # split the text into [ms_text, ms_number, ms_text, ms_number, ...]:
    body_split = re.split("(" + ms_regex + ")", body_text)
    while i < len(body_split) - 2: # last item in body_split is "" and one-but-last is the last msID, so we skip them
        counter += 1
        ms_text = body_split[i]
        ms_no = body_split[i + 1]
        
        # create an ID for the milestone from the file_id and the milestone number
        # e.g., Shamela00001-ara1.completed.ms005
        new_id = file_id + "." + ms_no

        # clean the text by normalizing
        # alifs, hamzas, ya's and Persian characters
        # and removing all Latin characters,
        # numbers and non-word characters (except spaces):
        text = funcs.text_cleaner(ms_text)

        # create a numerical version of the milestone number:
        seq = int(re.sub('[^0-9]', "", body_split[i + 1]))

        # fill in the record template and append it to the list:
        rec = rec_template % (new_id, file_id, text, seq)
        records.append(rec)

        # write to file if the maximum number of records per file has been reached:
        if counter % threshold == 0:
            with open(target_path + "-%05d" % counter, "w", encoding="utf8") as ft:
                ft.write("\n".join(records))
            records = []
        # update the data index to jump to the next data cell
        # since body_split contains both milestone texts and IDs
        i += 2

    # write any remaining records to file:
    counter_final = funcs.roundup(counter, threshold)
    with open(target_path + "-%05d" % counter_final, "w", encoding="utf8") as ft:
        ft.write("\n".join(records))

    return 1

def check_pri_ids(full_id, pri_list):
    """Check whether a version ID is in the list of primary versions"""
    b_id = full_id.split(".")[2] # get the last part of the name, book id, e.g., JK00001-ara1.inProgress
    id_nr = re.split("-\w{3}\d{1}(\.(mARkdown|completed|inProgress))?$", b_id)[0] # get the book id, e.g., JK00001 or JK00001A
    if re.search("[A-Z]{1}$", id_nr):
        id_nr = id_nr[:-1]
    if id_nr in pri_list:
        return True
    else:
        return False

def process_files(main_folder, output_folder, metadata_file, filter_pri_books, include_book_uri,
                  text_file_name_regex, milestone_regex, splitter, thresh, overwrite):
    print()
    print("Generating passim input files...")
    print()

    # create a set of all primary versions from the OpenITI metadata
    # if you want to filter based on primary books:
    if filter_pri_books:
        # load the OpenITI metadata into a pandas dataframe:
        metadata = pd.read_csv(metadata_file, delimiter="\t")  # ), index_col=1, skiprows=1).T.to_dict()

        # generate a list of the versions that have primary analysis priority:
        pri_metadata = metadata[metadata["status"] == "pri"]
        pri_books = set(pri_metadata['id'])
        #print("pri_books: ", pri_books)
    else:
        pri_books = []

    # convert all the relevant texts into linejson format:
    converted_files = []
    count = 1
    for root, dirs, files in os.walk(main_folder):
        # select the text files from the main folder
        # (based on the file name structure):
        texts = [f for f in files if re.search(text_file_name_regex, f)]
        
        if len(texts) > 0:
            # select only the primary versions if required:
            if filter_pri_books:
                texts = list(filter(lambda x: check_pri_ids(x, pri_books), texts))
                print("pri_text: ", texts)
                
            # group the same versions with different extensions:
            grouped_ids = [
                list(items) for gr, items in
                    groupby(
                        sorted(texts),
                        key=lambda name: name.split("-")[0]
                    )
            ]

            # organise the grouped versions in a dictionary
            # (key: version ID without language component and extension,
            #  value: list of text files)
            id_ext_dict = {}
            for g in grouped_ids:
                g = sorted(g) # when sorted, the first item will be the no-extension file
                key = g[0].split("-")[0]
                id_ext_dict[key] = g

            # 
            processed_ids = []
            for key, value in id_ext_dict.items():
                if key not in processed_ids:
                    if re.search("[A-Z]{1}$", key):
                        # merge text files that are split because of their size
                        # (The version ID of such files ends with a capital letter A, B, ...
                        # e.g., 1111Majlisi.BiharAnwar.Shia001432VolsA-ara1,
                        # 1111Majlisi.BiharAnwar.Shia001432VolsB-ara1):

                        # get a list of all parts of the current file:
                        multiple_ids = [(k,v) for k,v in id_ext_dict.items() if k[:-1] == key[:-1]]
                        # mark all of them as processed:
                        processed_ids.extend([x[0] for x in multiple_ids])

                        # choose the text file with the most developed annotation for each part
                        # (.mARkdown > .completed > .inProgress > (no extension)):
                        selected_parts = []
                        for id_ext in multiple_ids:
                            if id_ext[1][0] + ".mARkdown" in id_ext[1]:
                                selected_parts.append(id_ext[1][0] + ".mARkdown")
                            elif id_ext[1][0] + ".completed" in id_ext[1]:
                                selected_parts.append(id_ext[1][0] + ".completed")
                            elif id_ext[1][0] + ".inProgress" in id_ext[1]:
                                selected_parts.append(id_ext[1][0] + ".inProgress")
                            else:
                                selected_parts.append(id_ext[1][0])

                        # generate a single name to name the json files and records for passim inputs
                        # by removing the additional capital letter: e.g.,
                        # 1111Majlisi.BiharAnwar.Shia001432VolsA-ara1 > Shia001432Vols-ara1
                        # First, get the version ID by removing the author and title components of the URI:
                        #txt_id = re.sub("^\d{4}\w+\.\w+\.", "", selected_parts[0])  
                        author, title, txt_id = re.split("\.", selected_parts[0], maxsplit=2)
                        book_uri = author + "." + title
                        # Next, we remove "A", "B", "C", ... from the file names
                        txt_name_parts = txt_id.split("-")
                        txt_id = txt_name_parts[0][:-1] + "-" + txt_name_parts[1]

                        # Merge the text of the selected partial files into a single text:
                        tmp_data = []
                        for part in sorted(selected_parts):
                            with open(os.path.join(root, part), "r", encoding="utf8") as f1:
                                multi_part_data = f1.read()
                                tmp_data.append(multi_part_data.split(splitter)[1])
                        data = "\n".join(tmp_data)


                    else:
                        # choose the text file with the most developed annotation
                        # (.mARkdown > .completed > .inProgress > (no extension)):
                        no_ext_file = sorted(id_ext_dict[key])[0] # the first one in a sorted list is the no-extension file
                        if no_ext_file + ".mARkdown" in id_ext_dict[key]:
                            txt_full_name = no_ext_file + ".mARkdown"
                        elif g[0] + ".completed" in id_ext_dict[key]:
                            txt_full_name = no_ext_file + ".completed"
                        elif g[0] + ".inProgress" in id_ext_dict[key]:
                            txt_full_name = no_ext_file + ".inProgress"
                        else:
                            txt_full_name = no_ext_file
                            
                        # generate an ID by removing the author and title components of the URI:
                        #txt_id = re.sub("^\d{4}\w+\.\w+\.", "", txt_full_name)
                        author, title, txt_id = re.split("\.", txt_full_name, maxsplit=2)
                        book_uri = author + "." + title

                        # open the text file and split off the metadata header:
                        with open(os.path.join(root, txt_full_name), "r", encoding="utf8") as f1:
                            data = f1.read()
                            print("txt full name: ", txt_full_name)
                            data = data.split(splitter)[1]
                        
                    # chunk the text into milestones and store them in linejson format:
                    result_status = text_to_json(book_uri, txt_id, data, output_folder,
                                                 milestone_regex, thresh, overwrite, include_book_uri)

                    # log the progress:
                    count += result_status
                    converted_files.append([re.sub("\d{4}\w+\.\w+\.", "", txt_id), result_status])
                    if count % 100 == 0:
                        print()
                        print("=============" * 2)
                        print("Processed: %d" % count)
                        print("=============" * 2)
                        print()

    with open("corpus_log.csv", "w") as log_f:
        log_writer = csv.writer(log_f)
        log_writer.writerow(["book_id", "write_status"])
        log_writer.writerows(converted_files)
    print("Done!")


if __name__ == '__main__':
    # initialize parameters:
    text_file_name_regex = milestone_regex = splitter = thresh = overwrite = None
    main_folder = output_folder = metadata_file = filter_pri_books = include_book_uri = None

    # import parameters from a configuration file ../config.py:
    try:
        # add the parent directory of the current file to the PYTHONPATH:
        sys.path.insert(0, '..')
        # try importing the parameters from a config file
        from config import *
    except:
        print("no configuration file found")

    print("include_book_uri:", include_book_uri)

    # specify parameters not provided in the config file:
    if not text_file_name_regex:
        text_file_name_regex = r"^\d{4}\w+\.\w+\.\w+-\w{3}\d{1}(\.(mARkdown|completed|inProgress))?$"
    if not milestone_regex:
        milestone_regex = r"ms[A-Z]?\d+"
    if not splitter:
        splitter = "#META#Header#End#"
    if not thresh:
        thresh = 1000
    if not overwrite:
        overwrite = False
        
    
    # ask user input for input folders and pri/sec filtering:
    while (main_folder is None or not os.path.exists(main_folder)):
        print("no valid path was provided for the folder containing the texts:", main_folder)
        main_folder = input("Enter the path to the OpenITI folder: ").strip()
    while (output_folder is None or not os.path.exists(output_folder)):
        print("no valid path was provided for the folder with json files for passim input:", main_folder)
        output_folder = input("Enter the path to write the passim input files: ").strip()
    while (filter_pri_books not in [True, False]):
        filter_pri_books = input("Do you want to filter primary books (Y or n)? ").strip()
        if filter_pri_books.capitalize() == "Y":
            filter_pri_books = True
        elif filter_pri_books.capitalize() == "N":
            filter_pri_books = False
    if metadata_file is None:
        metadata_file = input("Enter the path to the metadata file: ").strip()
    if include_book_uri is None and not filter_pri_books:
        print("Do you want to include the book URI in the passim documents")
        include_book_uri = input("(for comparing only different versions of the same text)? Y or n: ").strip()
        if include_book_uri.capitalize() == "Y":
            include_book_uri = True
        else:
            include_book_uri = False

    process_files(main_folder, output_folder, metadata_file, filter_pri_books, include_book_uri,
                  text_file_name_regex, milestone_regex, splitter, thresh, overwrite)



