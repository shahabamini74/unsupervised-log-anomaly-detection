import sys

from logparser import Drain

import os

current_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(current_dir, "../1.split_data/")  # The input directory of log file
output_dir = os.path.join(current_dir, "../2.parsing/output_parsing/")  # The output directory of parsing results
log_file_all = "HDFS.log"  # The input log file name
log_file_train = "HDFS_train.log"  # The input log file name containing only the training data
log_format = "<Date> <Time> <Pid> <Level> <Component>: <Content>"  # HDFS log format
# Regular expression list for optional preprocessing (default: [])
regex = [
    r"blk_(|-)[0-9]+",  # block id
    r"(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)",  # IP
    r"(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$",  # Numbers
]
st = 0.5  # Similarity threshold
depth = 4  # Depth of all leaf nodes

import pandas as pd

def post_process_ids(output_dir, log_file_name):
    templates_file = os.path.join(output_dir, log_file_name + "_templates.csv")
    structured_file = os.path.join(output_dir, log_file_name + "_structured.csv")

    if os.path.exists(templates_file):
        df_templates = pd.read_csv(templates_file)
        # Create mapping
        event_id_map = {}
        for idx, row in df_templates.iterrows():
            event_id_map[row['EventId']] = f'E{idx + 1}'
        
        # Update templates
        df_templates['EventId'] = df_templates['EventId'].map(event_id_map)
        df_templates.to_csv(templates_file, index=False)

        # Update structured logs
        if os.path.exists(structured_file):
            df_structured = pd.read_csv(structured_file)
            df_structured['EventId'] = df_structured['EventId'].map(event_id_map)
            df_structured.to_csv(structured_file, index=False)
            print(f"Renamed EventIds in {log_file_name} files.")

if __name__ == "__main__":
    # run on training dataset
    parser = Drain.LogParser(
        log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex
    )
    parser.parse(log_file_all)

    # run on complete dataset
    parser = Drain.LogParser(
        log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex
    )
    parser.parse(log_file_train)

    post_process_ids(output_dir, log_file_all)
    post_process_ids(output_dir, log_file_train)