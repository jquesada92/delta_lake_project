
from config import * 

from glob import glob

import os
import shutil
import subprocess
import sys
import time
from datetime import datetime as dt

os.makedirs(RAW_PATH, exist_ok=True)


def speed_test():

    def move_temp_to_raw():
        files = glob(f"{TEMP_PATH}*.json")
        list(map(lambda x: os.rename(x, x.replace(TEMP_PATH, RAW_PATH)), files))

    now = dt.now()
    file_name = f"log_{now.strftime('%Y_%m_%d_%H_%M_%s')}.json"
    subprocess.run(
        f"touch {TEMP_PATH}{file_name} && speedtest --format=json >> {TEMP_PATH}{file_name} && mv  {TEMP_PATH}{file_name} {RAW_PATH}{file_name}",
        shell=True,
        executable="/bin/bash",
    )
    
    move_temp_to_raw()
    
def remove_empty_files():
    
    list(
            map(
                os.remove,
                filter(
                    lambda x: os.path.getsize(x) == 0,
                    glob("speed_test/raw_data/*.json"),
                ),
            )
        )



remove_empty_files()
speed_test()
