import os
import dotenv
import requests
import requests_ntlm
import pandas as pd
from http import HTTPStatus
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

dotenv.load_dotenv()


def parse_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return None


def write_log(msg: str) -> None:
    print(f"{msg}\n", end="")


domain = os.getenv("NTLM_DOMAIN")
username = os.getenv("NTLM_USER")
password = os.getenv("NTLM_PASSWORD")
base_url = os.getenv("NAV_URL")
max_single_fetch_count = parse_int(os.getenv("MAX_SINGLE_FETCH_COUNT")) or 5
start_item_index = parse_int(os.getenv("START_ITEM_INDEX")) or 0
max_item_count = parse_int(os.getenv("MAX_ITEM_COUNT")) or 1000
max_thread_count = parse_int(os.getenv("MAX_THREAD_COUNT")) or 2
max_try_count = parse_int(os.getenv("MAX_TRY_COUNT")) or 3
output_folder = os.getenv("OUTPUT_FOLDER")

# printing options

write_log(f"domain: {domain}")
write_log(f"username: {username}")
write_log(f"password: {password}")
write_log(f"base_url: {base_url}")
write_log(f"max_single_fetch_count: {max_single_fetch_count}")
write_log(f"start_item_index: {start_item_index}")
write_log(f"max_item_count: {max_item_count}")
write_log(f"max_thread_count: {max_thread_count}")
write_log(f"max_try_count: {max_try_count}")
write_log(f"output_folder: {output_folder}")

session = requests.Session()
session.auth = requests_ntlm.HttpNtlmAuth(
    username=f"{domain}\\{username}", password=password
)


def parse_xml(xml: str) -> pd.DataFrame:

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


def write_df(name: str, df: pd.DataFrame) -> None:

    file_path = f"{output_folder}/{name}.csv"

    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)

    df.to_csv(file_path, index=False)


def fetch_and_write(count: int, skip: int):

    url = f"{base_url}?$top={count}&$skip={skip}"
    response = session.get(url=url)

    if response.status_code != HTTPStatus.OK:
        raise Exception(f"{response}")

    xml_data = response.text

    df = parse_xml(xml=xml_data)
    write_df(name=f"sales_invoice_line_{skip}_{skip + count}", df=df)


def task(count: int, skip: int):
    try_count = 0
    base_msg = f"fetching {skip}...{skip + count}"

    while True:
        try:
            write_log(f"{base_msg}...")

            fetch_and_write(count=count, skip=skip)

            write_log(f"{base_msg} done.")
            break

        except Exception as ex:

            if try_count < max_try_count:
                try_count += 1
                write_log(f"{base_msg}, error: {ex}, trying again...")
            else:
                write_log(f"{base_msg}, error: {ex}")
                break


executer = ThreadPoolExecutor(max_workers=max_thread_count)

for i in range(start_item_index, max_item_count, max_single_fetch_count):
    count = min(max_single_fetch_count, max_item_count - i)
    executer.submit(task, count, i)

executer.shutdown()
