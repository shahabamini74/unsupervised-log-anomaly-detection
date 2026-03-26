import pandas as pd
import numpy as np
from collections import OrderedDict
import regex as re
from sliding_window_processor import FeatureExtractor, sequence_padder, windower
from collections import Counter

input_data = pd.read_csv("/Users/shahab/Desktop/anomal_XXXX/2.parsing/output_parsing/HDFS.log_structured.csv")

def collect_event_ids(data_frame, regex_pattern, column_names):
    """
    turns input data_frame into a 2 columned dataframe
    with columns: BlockId, EventSequence
    where EventSequence is a list of the events that happened to the block
    """
    import time
    data_dict = OrderedDict()
    
    # Pre-compile regex for performance
    pattern = re.compile(regex_pattern)
    
    # Convert columns to numpy arrays or lists for fast iteration
    # This avoids the massive overhead of creating a Series for each row in iterrows()
    contents = data_frame["Content"].values
    event_ids = data_frame["EventId"].values
    
    total_rows = len(contents)
    print(f"Processing {total_rows} rows...")
    start_time = time.time()
    
    # Iterate using zip
    for i, (content, event_id) in enumerate(zip(contents, event_ids)):
        # findall matches
        # ensure content is string
        blk_id_list = pattern.findall(str(content))
        blk_id_set = set(blk_id_list)
        
        for blk_id in blk_id_set:
            if blk_id not in data_dict:
                data_dict[blk_id] = []
            data_dict[blk_id].append(event_id)
            
        if (i + 1) % 100000 == 0:
            print(f"Processed {i + 1}/{total_rows} rows. Time elapsed: {time.time() - start_time:.2f}s")

    print(f"Finished processing. Converting to DataFrame...")
    data_df = pd.DataFrame(list(data_dict.items()), columns=column_names)
    return data_df

re_pat = r"(blk_-?\d+)"
col_names = ["BlockId", "EventSequence"]
events_df = collect_event_ids(input_data, re_pat, col_names) # taking a subset for demonstrative purposes


# Save the result to out_extraction
output_path = "3.feature_extraction/output_extraction/HDFS_train_event_sequence.csv"
# Convert list of events to string representation if needed, or keep as string representation of list
# The user asked for "sequence of event". The current list format is fine, or space separated string.
# Default pandas to_csv handles lists but it looks like stringified list "['E1', 'E2']".
# Usually for these tasks a space separated string is better?
# But per "sequence of event", I will stick to what the code generates (list) or convert to space separated if standard.
# Given I don't know the downstream preference, I'll stick to default csv behavior or maybe JSON-like string.
# Actually, let's just save the dataframe. The user didn't specify format details beyond "sequence of event".
events_df.to_csv(output_path, index=False)
print(f"Saved event sequences to {output_path}")

# Optional: Display head to verify
print(events_df.head())