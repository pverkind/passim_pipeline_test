"""Configuration for the passim text reuse pipeline"""


text_filename_regex = r"^\d{4}\w+\.\w+\.\w+-\w{3}\d{1}(\.(mARkdown|completed|inProgress))?$"
milestone_regex = r"ms[A-Z]?\d+"
splitter = "#META#Header#End#"
thresh = 1000
remilestone = False
filter_pri_books = True
include_book_uri=False

main_folder = "../texts"
output_folder = "../passim-input"
metadata_file = "../meta/OpenITI_Github_clone_metadata_light.csv"

