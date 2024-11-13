import os
import pandas as pd
import sys


def combine_csv_files(folder_path, output_csv):
    csv_files = [f for f in os.listdir(folder_path) if f[-4::1] == ".csv"]
    dataframes = []
    print(csv_files)

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df = combined_df.drop_duplicates()
    combined_df.to_csv(output_csv, index=False)

    print(f"combined csv saved to {output_csv}")


folder_path = sys.argv[0]
output_csv = sys.argv[1]
combine_csv_files(folder_path, output_csv)
