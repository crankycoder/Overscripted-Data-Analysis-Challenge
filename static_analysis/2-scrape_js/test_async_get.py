# Original code: Glen Thompson Sep 10 '17
#       https://stackoverflow.com/users/3866246/glen-thompson
#
# Script adapted by David Dobre Nov 14 '18:
#       Added parquet loading, iteration over dataframes, and content saving
################################################################################

from pathlib import Path
import concurrent.futures
import os
import os.path
import pandas as pd
import requests
import sys
import time

################################################################################
out = []
CONNECTIONS = 20
TIMEOUT = 2

# Get the main directory (this directory)
MAIN_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT_DIR = MAIN_DIR + "js_source_files/"

# # Small sample
# URL_LIST = MAIN_DIR + 'resources/url_master_list.csv'
# input_data = pd.read_csv(URL_LIST);
# input_data['script_url'] = input_data['url'] # just for laziness

# # Larger dataset
URL_LIST = os.path.join(MAIN_DIR, "full_url_list_v2")
#

PARQUET_DIR = Path(URL_LIST)
input_data = pd.concat(
    pd.read_parquet(parquet_file) for parquet_file in PARQUET_DIR.glob("*.parquet")
)


################################################################################


def load_url(url, filename, timeout):
    response = requests.get(url, timeout=timeout)

    if response.status_code == 200:
        content = response.text
        sys.stdout.write("[%d]" % len(content))
        sys.stdout.flush()
        return response.status_code, content, filename
    sys.stdout.write("o")
    sys.stdout.flush()

    # Don't forget to cast status code to int. Computers are the suck.
    return int(response.status_code), "", filename


################################################################################
with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (
        executor.submit(
            load_url, row["script_url"], OUTPUT_DIR + row["filename"], TIMEOUT
        )
        for index, row in input_data.iterrows()
    )

    time1 = time.time()

    for future in concurrent.futures.as_completed(future_to_url):
        http_status, content, filename = future.result()
        sys.stdout.write("O")
        sys.stdout.flush()
        if http_status == 200 and content:
            print(filename)
            with open(filename, "w") as source_file:
                source_file.write(content)
                sys.stdout.write("#")

    time2 = time.time()


################################################################################
# Summary
print("-" * 80)
print("Summary:\nIterated over:\t" + URL_LIST)
print(f"Took:\t\t{time2-time1:.2f} s")
print("-" * 80)
print(pd.Series(out).value_counts())
