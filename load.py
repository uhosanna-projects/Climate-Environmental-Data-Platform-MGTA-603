import pandas as pd
import glob

def load(folder_path):
    all_files = glob.glob(folder_path)

    dfs = []
    for filename in all_files:
        df = pd.read_csv(filename)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df