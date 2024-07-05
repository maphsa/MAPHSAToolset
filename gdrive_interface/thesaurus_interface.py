import requests
import pandas as pd

import gdrive_interface


def fetch_remote_thesaurus_data():
    for sheet_name in gdrive_interface.sheet_names:
        r = requests.get(f'https://docs.google.com/spreadsheets/d/{gdrive_interface.thesaurus_gdrive_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}')
        open(f'gdrive_interface/thesaurus_data/{sheet_name}.csv', 'wb').write(r.content)


def print_thesaurus_data(renew_data=False):
    if renew_data:
        fetch_remote_thesaurus_data()
    for sheet_name in gdrive_interface.sheet_names:
        print(sheet_name.upper())
        print("--------------------------------------")
        df = pd.read_csv(f"gdrive_interface/thesaurus_data/{sheet_name}.csv", header=None)
        thesauri_start_rows = list(df[df.iloc[:, 1] == 'Definitions'].index) + [len(df.index)]
        for thesaurus_start, thesaurus_end in zip(thesauri_start_rows, thesauri_start_rows[1:]):
            thesaurus_name = str(df.iloc[thesaurus_start, 0])
            thesaurus_values = df[thesaurus_start + 1:thesaurus_end].iloc[:, 0:2]
            thesaurus_values.columns = ["Value", "Description"]
            print(f"{thesaurus_name}\t{len(thesaurus_values)} values")
            print(thesaurus_values)
            print()
        continue


def get_online_thesaurus_data(renew_data=True):
    thesaurus_data = {}

    if renew_data:
        fetch_remote_thesaurus_data()
    for sheet_name in gdrive_interface.sheet_names:
        df = pd.read_csv(f"gdrive_interface/thesaurus_data/{sheet_name}.csv", header=None)
        thesauri_start_rows = list(df[df.iloc[:, 1] == 'Definitions'].index) + [len(df.index)]
        for thesaurus_start, thesaurus_end in zip(thesauri_start_rows, thesauri_start_rows[1:]):
            thesaurus_name = str(df.iloc[thesaurus_start, 0])
            thesaurus_values = df[thesaurus_start + 1:thesaurus_end].iloc[:, 0:2]
            thesaurus_values.columns = ["Value", "Description"]
            thesaurus_data[thesaurus_name] = [tuple(tv)[0] for tv in thesaurus_values.to_numpy()]

    return thesaurus_data

