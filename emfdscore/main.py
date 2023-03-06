import pandas as pd
import multiprocessing as mp
import os
import utils

from emfdscore.scoring import score_docs 

lock = mp.Lock()
dirname = os.path.dirname(__file__)

# multiprocessing parameters
NUMBER_OF_PROCESSES = 4
CHUNK_SIZE = 100000

# scoring parameters
DICT_TYPE = 'emfd'
PROB_MAP = 'all'
SCORE_METHOD = 'bow'
OUT_METRICS = 'sentiment'

# paths and file names 
OUT_CSV_PATH = "data_collection\\data\scores\\{}.csv" # rembember to add the \{}.csv at the end
IN_CSV_PATH = "data_collection\\data\\tweets\\{}.csv"
FILE_NAMES = ["UKLabour-regular", "Conservatives-regular"] # names of file inside IN_CSV_PATH folder

# headers to write to the file first
HEADERS = ["care_p", "fairness_p", "loyalty_p", "authority_p", 
    "sanctity_p", "care_sent", "fairness_sent", "loyalty_sent", 
    "authority_sent", "sanctity_sent", "moral_nonmoral_ratio", "f_var", 
    "sent_var", "user_id"]

def calculate_score(df_chunk, output_path):
    num_docs = len(df_chunk)

    tweets_text = df_chunk["tweet_text"].to_list()
    user_id = df_chunk["user_id"].to_list()

    tweets_text_df = pd.DataFrame(tweets_text)
    print(tweets_text_df)

    df = score_docs(tweets_text_df, DICT_TYPE, PROB_MAP, SCORE_METHOD, OUT_METRICS, num_docs)

    with lock:
        df.insert(loc=0, column='user_id', value=user_id)
        df.to_csv(path_or_buf=output_path, mode="a", index=False, header=None)

if __name__ == "__main__":
    df_chunks = {}

    # read every file
    for name in FILE_NAMES:
        print(f"Reading {name}")

        # set paths
        input_path = IN_CSV_PATH.format(name)
        output_path = OUT_CSV_PATH.format(name)
     
        # read csv
        df = pd.read_csv(input_path, on_bad_lines='skip', encoding='utf-8')
        df = df.astype("string")

        # split data
        df_chunks[name] = [df[i:i+CHUNK_SIZE].reset_index() for i in range(0, len(df), CHUNK_SIZE)]

        # create NUMBER_OF_PROCESSES subchunks
        chunks = df_chunks[name]
        subchunks = [chunks[i:i+NUMBER_OF_PROCESSES] for i in range(0, len(chunks), NUMBER_OF_PROCESSES)]

        # write file header
        output_file = open(output_path, "a", encoding="utf-8")
        utils.write_file(output_file, *HEADERS)
        output_file.close()

        # run processes
        for chunks in subchunks:
            processes = [mp.Process(target=calculate_score, args=(chunk, output_path)) for chunk in chunks]

            for p in processes:
                p.start()

            for p in processes:
                p.join()

    




