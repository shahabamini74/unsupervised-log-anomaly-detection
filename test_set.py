import pandas as pd
import numpy as np
import logging

import os

current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, '/Users/shahab/Desktop/anomal_XXXX/2.parsing/output_parsing/HDFS.log_structured.csv')
output_path = os.path.join(current_dir, '/Users/shahab/Desktop/anomal_XXXX/2.parsing/output_parsing/HDFS_test.log_structured.csv')

all_parsed = pd.read_csv(file_path)
train_idx = int(len(all_parsed) * 0.8)
test_parsed = all_parsed.iloc[train_idx:]
test_parsed.to_csv(output_path, index = False)