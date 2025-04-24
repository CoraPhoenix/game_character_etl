import sys
import logging
from datetime import datetime, timedelta

# importing etl functions
from etl_funcs.scraper import *
from etl_funcs.transform import *
from etl_funcs.loader import *

# Configuring logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

#  ========================================= Main functions ======================================== #

def extract_main_scraper() -> None:

    extract_wuwa_char_info_from_web()
    extract_genshin_char_info_from_web()
    extract_zzz_char_info_from_web()
    extract_hsr_char_info_from_web()
    extract_ow_char_info_from_web()

def transform_fix_scraped_data() -> None:

    transform_hsr_char_info()
    transform_ow_char_info()

def transform_main_convert_to_star_schema() -> None:

    transform_wuwa_csv_into_tables()
    transform_hoyo_csv_into_tables()
    transform_ow_csv_into_tables()

def load_main_tables_to_db() -> None:

    load_wuwa_tables_to_db()
    load_hoyo_tables_to_db()
    load_ow_tables_to_db()


if __name__ == "__main__":

    scraping = False

    from dotenv import load_dotenv

    load_dotenv()

    try:

        logging.info("Game Character Database Creation - Starting ETL process...")

        if scraping:
            extract_main_scraper()
            transform_fix_scraped_data()
        transform_main_convert_to_star_schema()
        load_main_tables_to_db()

        logging.info("Game Character Database Creation - ETL process finished successfully.")
    
    except Exception as e:
        logging.error(f"An error has occurred during ETL sequence: {e}")

