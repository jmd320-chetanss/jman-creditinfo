# %% [markdown]
# # Generating Session

# %%

import requests
import requests_ntlm
from bs4 import BeautifulSoup
from http import HTTPStatus
import os
import dotenv

domain = os.getenv("NTLM_DOMAIN")
username = os.getenv("NTLM_USER")
password = os.getenv("NTLM_PASSWORD")
base_url = os.getenv("NAV_URL")

session = requests.Session()
session.auth = requests_ntlm.HttpNtlmAuth(
    username=f"{domain}\\{username}", password=password
)

# ------------------------------------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------------------------------------


def list_files_in_folder(folder_path):
    files = []
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path):
            files.append(file_path)

    return files


# %% [markdown]
# # Parsing Sales Invoice Lines

# %%
from bs4 import BeautifulSoup
import pandas as pd


def parse_sales_invoice_lines(xml: str) -> pd.DataFrame:

    soup = BeautifulSoup(xml, features="xml")

    items = list()
    for properites in soup.find_all("m:properties") or []:

        item = dict()
        for child in properites.children:
            if child.name is None:
                continue

            item[child.name] = child.text

        items.append(item)

    df = pd.DataFrame(items)
    return df


def write_output(name: str, df: pd.DataFrame) -> None:
    file_path = f"out/datasets/{name}.csv"
    df.to_csv(file_path, index=False)


# %% [markdown]
# # Fetching Sales Invoice Lines

# %%

from concurrent.futures import ThreadPoolExecutor

max_single_fetch_count = 10000
start_item_index = 20000
max_item_count = 1988525
max_thread_count = 2

def handle(count: int, skip: int):

    print(f"fetching {skip}...{skip + count}...\n", end="")

    url = f"{base_url}?$top={count}&$skip={skip}"
    response = session.get(url=url)

    if response.status_code != HTTPStatus.OK:
        print(f"fetching failed, {skip}...{skip + count}, response: {response}")
        return

    xml_data = response.text

    df = parse_sales_invoice_lines(xml=xml_data)
    write_output(name=f"sales_invoice_{skip}_{skip + count}", df=df)

    print(f"fetching {skip}...{skip + count} done.\n", end="")


executer = ThreadPoolExecutor(max_workers=max_thread_count)
for i in range(start_item_index, max_item_count, max_single_fetch_count):
    count = min(max_single_fetch_count, max_item_count - i)
    executer.submit(handle, count, i)

executer.shutdown()


# class Options:
#     output_folder = "./out/datasets"


# options = Options()

# file_names = list_files_in_folder(options.output_folder)
