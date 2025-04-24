from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import pandas as pd
import logging
import random
import time
import sys
from datetime import datetime, timedelta
import logging
import psycopg2
import os

from airflow import DAG
from airflow.operators.python import PythonOperator 

# Configuring logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

#  ============================== Extraction functions - Web Scraping ============================== #

def extract_wuwa_char_info_from_web() -> None:

    """
    This function extracts info from playable characters belonging to the game Wuthering Waves
    from the Fandom wiki. It gets a list of currently active characters and, for each character,
    it enters their Fandom wiki entry, get their name, gender, location and release date, and add
    the info to a list. Then, after collecting info from all characters, it converts the generated
    list to a pandas DataFrame and save it as a CSV file.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Starting collection of character data from game: Wuthering Waves...")

    # 1. Getting list of active characters

    char_info = []
    with open("data_input/character_list_wuwa.txt", "r") as cf:
        char_names = cf.readlines()

    # 2. Extracting info from each character

    for character in char_names:

        character = character.replace('\n', '') # removes next line character so that it can be used in the URL

        logging.info(f"Extracting info for character: {character}")

        url = f"https://wutheringwaves.fandom.com/wiki/{character.replace(' ', '_')}"
        headers = {'User-Agent': UserAgent().random}
        data_request = requests.get(url, headers=headers)

        if data_request.status_code == 200:
            webpage = BeautifulSoup(data_request.text, "html.parser")
            character_name = webpage.find_all("h1")[0].get_text().strip()

            try:
                character_gender = webpage.find_all("div", attrs={"data-source" : "gender"})[0].find("div").get_text().strip()
                character_region = webpage.find_all("div", attrs={"data-source" : "nation"})[0].find("div").get_text().strip()
                character_release_date = webpage.find_all("div", attrs={"data-source" : "releaseDate"})[0].find("div").get_text(separator = "\n").split("\n")[0]
            except IndexError as ie:
                if character == "Rover": # Rover is the player's character; they can be a male or a female
                    character_gender = "Any"
                    character_region = webpage.find_all("div", attrs={"data-source" : "nation"})[0].find("div").get_text().strip()
                    character_release_date = webpage.find_all("div", attrs={"data-source" : "releaseDate"})[0].find("div").get_text(separator = "\n").split("\n")[0]
                else:
                    # the block below adds birthplace as a regio info instead of nation
                    character_gender = webpage.find_all("div", attrs={"data-source" : "gender"})[0].find("div").get_text().strip()
                    character_region = webpage.find_all("div", attrs={"data-source" : "birthplace"})[0].find("div").get_text().strip()
                    character_release_date = webpage.find_all("div", attrs={"data-source" : "releaseDate"})[0].find("div").get_text(separator = "\n").split("\n")[0]

            # gathering character info and adding it to a dictionary, then appending it to a list
            char_content = {
                "name": character_name,
                "gender": character_gender if character_gender else "Any",
                "region": character_region if character_region else "Unknown",
                "release_date": character_release_date if character_release_date else "Unknown"
            }
            char_info.append(char_content)

            # random delay to minimise risk of being detected as scraper
            delay = random.uniform(5, 15)
            logging.info(f"Character info collected successfully. Waiting {delay} seconds before proceeding...")
            time.sleep(delay)

        elif data_request.status_code in [403, 429]: # If the program is detected as a scraper, it is immediately terminated
            logging.fatal("System detected as a scraper! Terminating program...")
            sys.exit()
        else:
            print(f"Could not retrieve page content. The following error was returned: {data_request.reason}")

    # 3. Converting structured list into a DataFrame then saving into a CSV file
    wuwa_char_df = pd.DataFrame(char_info)
    wuwa_char_df.to_csv("temp/wuthering_waves_character_data.csv")

    logging.info("Info of characters successfully extracted. Finishing program...")

def extract_genshin_char_info_from_web() -> None:

    """
    This function extracts info from playable characters belonging to the game Genshin Impact
    from the Fandom wiki. It accesses the character list from the site, then, for each character in
    the general playable character list, get their name, gender, location and release date, and add
    the info to a list. Finally, after collecting info from all characters, it converts the generated
    list to a pandas DataFrame and save it as a CSV file.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Starting collection of character data from game: Genshin Impact...")

    char_info = []

    logging.info(f"Extracting character info table...")

    # 1. Accessing character list from Fandom wiki

    url = f"https://genshin-impact.fandom.com/wiki/Character/List"
    headers = {'User-Agent': UserAgent().random}
    data_request = requests.get(url, headers=headers)
    
    # 2. Extracting info from each character

    if data_request.status_code == 200:
        webpage = BeautifulSoup(data_request.text, "html.parser")
        character_list = webpage.find_all("tbody")[0].find_all("tr")[1:]

        for character in character_list:
            # gathering character info and adding it to a dictionary, then appending it to a list
            character_name = character.find_all("td")[1].get_text()
            character_gender = character.find_all("td")[-3].get_text()
            character_region = character.find_all("td")[-4].get_text().strip()
            character_release_date = character.find_all("td")[-2].get_text()

            char_content = {
                "name": character_name.strip(),
                "gender": "Any" if character_name.strip() == "Traveler" else character_gender.strip().split(" ")[1], # Traveler is the player's character, so they can be a male or a female
                "region": "Unknown" if character_region == "None" else character_region, # Some characters are from unknown regions, like Traveler and Aloy
                "release_date": character_release_date.strip() if character_release_date else "Unknown"
            }
            char_info.append(char_content)

    elif data_request.status_code in [403, 429]: # If the program is detected as a scraper, it is immediately terminated
        logging.fatal("System detected as a scraper! Terminating program...")
        sys.exit()
    else:
        print(f"Could not retrieve page content. The following error was returned: {data_request.reason}")

    # 3. Converting structured list into a DataFrame then saving into a CSV file
    genshin_char_df = pd.DataFrame(char_info)
    genshin_char_df.to_csv("temp/genshin_impact_character_data.csv")

    logging.info("Info of characters successfully extracted. Finishing program...")

def extract_zzz_char_info_from_web() -> None:

    """
    This function extracts info from playable characters belonging to the game Zenless Zone Zero
    from the Fandom wiki. It gets a list of currently active characters and, for each character,
    it enters their Fandom wiki entry, get their name, gender, location and release date, and add
    the info to a list. Then, after collecting info from all characters, it converts the generated
    list to a pandas DataFrame and save it as a CSV file.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Starting collection of character data from game: Zenless Zone Zero...")

    # 1. Getting list of active characters

    char_info = []
    with open("data_input/character_list_zzz.txt", "r") as cf:
        char_names = cf.readlines()

    # 2. Extracting info from each character

    for character in char_names:

        character = character.replace('\n', '') # removes next line character so that it can be used in the URL

        logging.info(f"Extracting info for character: {character}")

        url = f"https://zenless-zone-zero.fandom.com/wiki/{character.replace(' ', '_')}"
        headers = {'User-Agent': UserAgent().random}
        data_request = requests.get(url, headers=headers)

        if data_request.status_code == 200:
            webpage = BeautifulSoup(data_request.text, "html.parser")
            character_name = webpage.find_all("span", class_ = "mw-page-title-main")[0].get_text().strip()
            
            character_gender = webpage.find_all("div", attrs={"data-source" : "gender"})[0].find("div").get_text().strip()
            character_faction = webpage.find_all("div", attrs={"data-source" : "faction"})[0].find("div").get_text().strip()
            character_release_date = webpage.find_all("div", attrs={"data-source" : "releaseDate"})[0].find("div").get_text(separator = "\n").split("\n")[0]
            
            # gathering character info and adding it to a dictionary, then appending it to a list
            char_content = {
                "name": character_name,
                "gender": character_gender if character_gender else "Any",
                "faction": character_faction[:(len(character_faction) // 2 + 1)].strip() if character_faction else "Unknown",
                "release_date": character_release_date if character_release_date else "Unknown"
            }
            char_info.append(char_content)

            # random delay to minimise risk of being detected as scraper
            delay = random.uniform(5, 15)
            logging.info(f"Character info collected successfully. Waiting {delay} seconds before proceeding...")
            time.sleep(delay)

        elif data_request.status_code in [403, 429]: # If the program is detected as a scraper, it is immediately terminated
            logging.fatal("System detected as a scraper! Terminating program...")
            sys.exit()
        else:
            print(f"Could not retrieve page content. The following error was returned: {data_request.reason}")

    # 3. Converting structured list into a DataFrame then saving into a CSV file
    zzz_char_df = pd.DataFrame(char_info)
    zzz_char_df.to_csv("temp/zenless_zone_zero_character_data.csv")

    logging.info("Info of characters successfully extracted. Finishing program...")

def extract_hsr_char_info_from_web() -> None:

    """
    This function extracts info from playable characters belonging to the game Honkai: Star Rail
    from the Fandom wiki. First, it retrieves a list of currently active characters from another 
    website, then, for each character, it enters their Fandom wiki entry, get their name, gender, 
    location and release date, and add the info to a list. Then, after collecting info from all 
    characters, it converts the generated list to a pandas DataFrame and save it as a CSV file.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Starting collection of character data from game: Honkai: Star Rail...")

    char_info = []

    logging.info(f"Extracting character info table...")

    ##### A. Getting character list

    # 1. Accessing character list from The Gamer

    url = f"https://www.thegamer.com/honkai-star-rail-playable-character-age-height-path-element/"
    headers = {'User-Agent': UserAgent().random}
    data_request = requests.get(url, headers=headers)
    
    # 2. Extracting info from character table

    if data_request.status_code == 200:
        webpage = BeautifulSoup(data_request.text, "html.parser")
        character_list = webpage.find_all("tbody")[0].find_all("tr")[1:]

        for character in character_list:
            # gathering character info and adding it to a dictionary, then appending it to a list
            character_name = character.find_all("td")[0].get_text()
            character_gender = character.find_all("td")[2].get_text()

            char_content = {
                "name": character_name.strip(),
                "gender": "Any" if character_name.strip() == "Trailblazer" else character_gender.strip().split(" ")[1], # Trailblazer is the player's character, so they can be a male or a female
            }
            char_info.append(char_content)

    elif data_request.status_code in [403, 429]: # If the program is detected as a scraper, it is immediately terminated
        logging.fatal("System detected as a scraper! Terminating program...")
        sys.exit()
    else:
        print(f"Could not retrieve page content. The following error was returned: {data_request.reason}")

    ##### B. Getting info for each character

    for character in char_info:

        char_name = character["name"]

        logging.info(f"Extracting info for character: {char_name}")

        url = f"https://honkai-star-rail.fandom.com/wiki/{char_name.replace(' ', '_').replace('and', '%26')}"
        headers = {'User-Agent': UserAgent().random}
        data_request = requests.get(url, headers=headers)

        if data_request.status_code == 200:
            webpage = BeautifulSoup(data_request.text, "html.parser")

            character_faction = webpage.find_all("div", attrs={"data-source" : "faction"})[0].find("div").get_text().strip()
            character_release_date = webpage.find_all("div", attrs={"data-source" : "release_date"})[0].find("div").get_text().strip()

            # gathering character info and adding it to a dictionary, then appending it to a list
            character["faction"] = character_faction
            character["release_date"] = character_release_date

            # random delay to minimise risk of being detected as scraper
            delay = random.uniform(5, 15)
            logging.info(f"Character info collected successfully. Waiting {delay} seconds before proceeding...")
            time.sleep(delay)

        elif data_request.status_code in [403, 429]: # If the program is detected as a scraper, it is immediately terminated
            logging.fatal("System detected as a scraper! Terminating program...")
            sys.exit()
        else:
            print(f"Could not retrieve page content. The following error was returned: {data_request.reason}")

    # 3. Converting structured list into a DataFrame then saving into a CSV file
    hsr_char_df = pd.DataFrame(char_info)
    hsr_char_df.to_csv("temp/honkai_star_rail_character_data.csv")

    logging.info("Info of characters successfully extracted. Finishing program...")

def extract_ow_char_info_from_web() -> None:

    """
    This function extracts info from playable characters belonging to the game Overwatch 2
    from the Fandom wiki. First, it gets a list of currently active characters, where gender 
    info is available, and converts it into a dictionary. Second, it accesses the character 
    list from the site. Then, for each character in the general playable character list, gets 
    their name, location and release date, and add the info to a list, using the dictionary
    to add the gender. Finally, after collecting info from all characters, it converts the 
    generated list to a pandas DataFrame and save it as a CSV file.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Starting collection of character data from game: Overwatch 2...")

    char_info = []
    char_gender_hash = {}
    with open("data_input/character_list_ow.txt", "r") as cf:
        char_names = cf.readlines()

    for character in char_names:
        character = character.replace('\n', '')
        char_name, char_gender = character.split("-")
        char_name = char_name.strip()
        char_gender = char_gender.strip()
        char_gender_hash[char_name] = char_gender

    logging.info(f"Extracting character info table...")

    # Accessing character list from Fandom wiki

    url = "https://overwatch.fandom.com/wiki/Heroes"
    headers = {'User-Agent': UserAgent().random}
    data_request = requests.get(url, headers=headers)
    
    # 2. Extracting info from each character

    if data_request.status_code == 200:
        webpage = BeautifulSoup(data_request.text, "html.parser")
        character_list = webpage.find_all("tbody")[1].find_all("tr")[1:]

        for character in character_list:
            # gathering character info and adding it to a dictionary, then appending it to a list

            # source table wasn't a perfect grid, so it was necessary to create a region index variable
            if len(character.find_all("td")) in [5, 6]:
                region_index = -1
            elif len(character.find_all("td")) == 7:
                region_index = -2
            else:
                region_index = -3

            try:
                character_name = character.find_all("td")[2].get_text()
                character_gender = "Male" if "Aqua" in character_name else char_gender_hash[character_name.replace("\n", "")]
                character_region = character.find_all("td")[region_index].get_text().strip()
                character_release_date = character.find_all("td")[-1].get_text()
            except KeyError as ke:
                # Why, Torbjörn?
                character_name = character.find_all("td")[2].get_text()
                character_gender = "Male"
                character_region = character.find_all("td")[region_index].get_text().strip()
                character_release_date = character.find_all("td")[-1].get_text()

            char_content = {
                "name": character_name.strip().replace("\n", ""),
                "gender": character_gender.strip(),
                "region": character_region.strip().replace("\n", ""),
                "release_date": character_release_date.strip() if character_release_date else "Unknown"
            }
            char_info.append(char_content)

    elif data_request.status_code in [403, 429]: # If the program is detected as a scraper, it is immediately terminated
        logging.fatal("System detected as a scraper! Terminating program...")
        sys.exit()
    else:
        print(f"Could not retrieve page content. The following error was returned: {data_request.reason}")

    # 3. Converting structured list into a DataFrame then saving into a CSV file
    ow_char_df = pd.DataFrame(char_info)
    ow_char_df.to_csv("temp/overwatch_2_character_data.csv")

    logging.info("Info of characters successfully extracted. Finishing program...")


#  ==================================== Transformation functions =================================== #

def transform_hsr_char_info() -> None:

    """
    This function gets the generated CSV containing info of Honkai: Star Rail characters and processes
    the faction and release_date fields for each character, removing unnecessary information. After filtering,
    it updates the transformed CSV.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Cleaning character data from game: Honkai: Star Rail...")

    # Load file
    hsr_char_df = pd.read_csv("temp/honkai_star_rail_character_data.csv")

    # Formatting and cleaning faction data
    top_faction_getter = lambda x: x.split("(")[0]
    hsr_char_df["faction"] = hsr_char_df["faction"].apply(top_faction_getter)

    # Formatting and cleaning release date
    release_date_getter = lambda x: x[:x.index(",") + 6]
    hsr_char_df["release_date"] = hsr_char_df["release_date"].apply(release_date_getter)

    # Saving transformed DataFrame
    hsr_char_df.to_csv("temp/honkai_star_rail_character_data.csv")

    logging.info("Cleaning done.")

def transform_ow_char_info() -> None:

    """
    This function gets the generated CSV containing info of Overwatch characters and processes
    the region and release_date fields for each character. After filtering,
    it updates the transformed CSV.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Cleaning character data from game: Overwatch 2...")

    # Load file
    ow_char_df = pd.read_csv("temp/overwatch_2_character_data.csv")

    # 1. Convert dataframe to a list of dictionaries
    char_info = []
    for row in ow_char_df.itertuples():
        char_info.append({"name" : row.name, 
                          "gender" : row.gender, 
                          "region" : row.region, 
                          "release_date" : row.release_date})

    # 2. The starting characters share the same release date, so it'll be saved in a varaible to reassign the value to the required fields
    release_date = None
    for char in char_info:
        if char["name"] == "Tracer": # Tracer is the first character in the list
            release_date = char["release_date"]
        if char["name"] != "Ana": # Ana is the first character to be released after game release, so she's not a starting character
            char["release_date"] = release_date
        else:
            break

    # 3. Specific characters require specific corrections
    for char in char_info:
        # fixing names
        if "2016" in char["release_date"] and char["region"] == "Brazil": # Lúcio
            char["name"] = "Lucio"
        if "2016" in char["release_date"] and char["region"] == "Sweden": # Torbjörn
            char["name"] = "Torbjorn"
        # fixing locations
        if char["name"] == "Wrecking Ball":
            char["region"] = "The Moon" # hamster is from the Moon
        if char["name"] == "Juno":
            char["region"] = "Mars" # Juno is from Mars
        # fixing genders
        if char["name"] == "Bastion":
            char["gender"] = "Genderless" # Bastion has no gender

    # 4. Convert list of dictionaries back to a dataframe
    ow_char_df = pd.DataFrame(char_info)

    # 5. Fixing date
    date_fixer = lambda x: x.split("(")[0].strip()
    ow_char_df["release_date"] = ow_char_df["release_date"].apply(date_fixer)

    # Saving transformed DataFrame
    ow_char_df.to_csv("temp/overwatch_2_character_data.csv")

    logging.info("Cleaning done.")

def transform_wuwa_csv_into_tables() -> None:

    """
    This function gets the CSV file containing the extracted data of Wuthering Waves characters
    and converts it to a list of CSV files which follow the star schema. In this case, both gender 
    and region are dimensions.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Generating star schema for character data from game: Wuthering Waves...")

    logging.info("Creating dimension tables...")

    # A. Getting dimension tables

    wuwa_char_df = pd.read_csv("temp/wuthering_waves_character_data.csv")

    wuwa_gender_df = wuwa_char_df.groupby(["gender"]).count().reset_index()
    wuwa_gender_df["gender_id"] = wuwa_gender_df.index
    wuwa_gender_df["gender_id"] = wuwa_gender_df["gender_id"].apply(lambda x: x + 1)
    wuwa_gender_df = wuwa_gender_df[["gender_id", "gender"]]

    wuwa_region_df = wuwa_char_df.groupby(["region"]).count().reset_index()
    wuwa_region_df["region_id"] = wuwa_region_df.index
    wuwa_region_df["region_id"] = wuwa_region_df["region_id"].apply(lambda x: x + 1)
    wuwa_region_df = wuwa_region_df[["region_id", "region"]]

    # B. Getting facts table

    logging.info("Creating facts table...")

    wuwa_facts_table = wuwa_char_df
    wuwa_facts_table.insert(0, "character_id", wuwa_facts_table.index + 1)
    #wuwa_facts_table["character_id"] = wuwa_facts_table["character_id"].apply(lambda x: x + 1)

    gender_assign_func = lambda x : wuwa_gender_df[wuwa_gender_df["gender"] == x]["gender_id"].values[0]
    wuwa_facts_table["gender"] = wuwa_facts_table["gender"].apply(gender_assign_func)

    region_assign_func = lambda x : wuwa_region_df[wuwa_region_df["region"] == x]["region_id"].values[0]
    wuwa_facts_table["region"] = wuwa_facts_table["region"].apply(region_assign_func)

    wuwa_facts_table["release_date"] = wuwa_facts_table["release_date"].apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime("%Y-%m-%d"))

    wuwa_facts_table = wuwa_facts_table.drop("Unnamed: 0", axis = 1)
    wuwa_facts_table.rename(columns={'gender': 'gender_id', 'region': 'region_id'}, inplace=True)

    # saving transformed data tables

    logging.info("Star schema tables generated. Saving tables...")

    wuwa_gender_df.to_csv("temp/wuwa_gender_df.csv")
    wuwa_region_df.to_csv("temp/wuwa_region_df.csv")
    wuwa_facts_table.to_csv("temp/wuwa_facts_table.csv")

    logging.info("Tables saved.")

def transform_hoyo_csv_into_tables() -> None:

    """
    This function gets the CSV file containing the extracted data of miHoYo/HoYoverse characters 
    and converts it to a list of CSV files which follow the star schema. In this case, both gender 
    and region/faction are dimensions.

    Parameters:
    None

    Returned value:
    None
    """

    ### ------------------- I. Genshin Impact characters ------------------- ###

    logging.info("Generating star schema for character data from game: Genshin Impact...")

    logging.info("Creating dimension tables...")

    # A. Getting dimension tables

    genshin_char_df = pd.read_csv("temp/genshin_impact_character_data.csv")

    genshin_gender_df = genshin_char_df.groupby(["gender"]).count().reset_index()
    genshin_gender_df["gender_id"] = genshin_gender_df.index
    genshin_gender_df["gender_id"] = genshin_gender_df["gender_id"].apply(lambda x: x + 1)
    genshin_gender_df = genshin_gender_df[["gender_id", "gender"]]

    genshin_region_df = genshin_char_df.groupby(["region"]).count().reset_index()
    genshin_region_df["region_id"] = genshin_region_df.index
    genshin_region_df["region_id"] = genshin_region_df["region_id"].apply(lambda x: x + 1)
    genshin_region_df = genshin_region_df[["region_id", "region"]]

    # B. Getting facts table

    logging.info("Creating facts table...")

    genshin_facts_table = genshin_char_df
    genshin_facts_table.insert(0, "character_id", genshin_facts_table.index + 1)

    gender_assign_func = lambda x : genshin_gender_df[genshin_gender_df["gender"] == x]["gender_id"].values[0]
    genshin_facts_table["gender"] = genshin_facts_table["gender"].apply(gender_assign_func)

    region_assign_func = lambda x : genshin_region_df[genshin_region_df["region"] == x]["region_id"].values[0]
    genshin_facts_table["region"] = genshin_facts_table["region"].apply(region_assign_func)

    genshin_facts_table["release_date"] = genshin_facts_table["release_date"].apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime("%Y-%m-%d"))

    genshin_facts_table = genshin_facts_table.drop("Unnamed: 0", axis = 1)
    genshin_facts_table.rename(columns={'gender': 'gender_id', 'region': 'region_id'}, inplace=True)

    # saving transformed data tables

    logging.info("Star schema tables generated. Saving tables...")

    genshin_gender_df.to_csv("temp/genshin_gender_df.csv")
    genshin_region_df.to_csv("temp/genshin_region_df.csv")
    genshin_facts_table.to_csv("temp/genshin_facts_table.csv")

    logging.info("Tables saved.")

    ### ----------------- II. Honkai: Star Rail characters ----------------- ###

    logging.info("Generating star schema for character data from game: Honkai: Star Rail...")

    logging.info("Creating dimension tables...")

    # A. Getting dimension tables

    hsr_char_df = pd.read_csv("temp/honkai_star_rail_character_data.csv")

    hsr_gender_df = hsr_char_df.groupby(["gender"]).count().reset_index()
    hsr_gender_df["gender_id"] = hsr_gender_df.index
    hsr_gender_df["gender_id"] = hsr_gender_df["gender_id"].apply(lambda x: x + 1)
    hsr_gender_df = hsr_gender_df[["gender_id", "gender"]]

    hsr_faction_df = hsr_char_df.groupby(["faction"]).count().reset_index()
    hsr_faction_df["faction_id"] = hsr_faction_df.index
    hsr_faction_df["faction_id"] = hsr_faction_df["faction_id"].apply(lambda x: x + 1)
    hsr_faction_df = hsr_faction_df[["faction_id", "faction"]]

    # B. Getting facts table

    logging.info("Creating facts table...")

    hsr_facts_table = hsr_char_df
    hsr_facts_table.insert(0, "character_id", hsr_facts_table.index + 1)

    gender_assign_func = lambda x : hsr_gender_df[hsr_gender_df["gender"] == x]["gender_id"].values[0]
    hsr_facts_table["gender"] = hsr_facts_table["gender"].apply(gender_assign_func)

    faction_assign_func = lambda x : hsr_faction_df[hsr_faction_df["faction"] == x]["faction_id"].values[0]
    hsr_facts_table["faction"] = hsr_facts_table["faction"].apply(faction_assign_func)

    hsr_facts_table["release_date"] = hsr_facts_table["release_date"].apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime("%Y-%m-%d"))

    hsr_facts_table = hsr_facts_table.drop(["Unnamed: 0", "Unnamed: 0.1"], axis = 1)
    hsr_facts_table.rename(columns={'gender': 'gender_id', 'faction': 'faction_id'}, inplace=True)

    # saving transformed data tables

    logging.info("Star schema tables generated. Saving tables...")

    hsr_gender_df.to_csv("temp/hsr_gender_df.csv")
    hsr_faction_df.to_csv("temp/hsr_faction_df.csv")
    hsr_facts_table.to_csv("temp/hsr_facts_table.csv")

    logging.info("Tables saved.")

    ### ---------------- III. Zenless Zone Zero characters ----------------- ###

    logging.info("Generating star schema for character data from game: Zenless Zone Zero...")

    logging.info("Creating dimension tables...")

    # A. Getting dimension tables

    zzz_char_df = pd.read_csv("temp/zenless_zone_zero_character_data.csv")

    zzz_gender_df = zzz_char_df.groupby(["gender"]).count().reset_index()
    zzz_gender_df["gender_id"] = zzz_gender_df.index
    zzz_gender_df["gender_id"] = zzz_gender_df["gender_id"].apply(lambda x: x + 1)
    zzz_gender_df = zzz_gender_df[["gender_id", "gender"]]

    zzz_faction_df = zzz_char_df.groupby(["faction"]).count().reset_index()
    zzz_faction_df["faction_id"] = zzz_faction_df.index
    zzz_faction_df["faction_id"] = zzz_faction_df["faction_id"].apply(lambda x: x + 1)
    zzz_faction_df = zzz_faction_df[["faction_id", "faction"]]

    # B. Getting facts table

    logging.info("Creating facts table...")

    zzz_facts_table = zzz_char_df
    zzz_facts_table.insert(0, "character_id", zzz_facts_table.index + 1)

    gender_assign_func = lambda x : zzz_gender_df[zzz_gender_df["gender"] == x]["gender_id"].values[0]
    zzz_facts_table["gender"] = zzz_facts_table["gender"].apply(gender_assign_func)

    faction_assign_func = lambda x : zzz_faction_df[zzz_faction_df["faction"] == x]["faction_id"].values[0]
    zzz_facts_table["faction"] = zzz_facts_table["faction"].apply(faction_assign_func)

    zzz_facts_table["release_date"] = zzz_facts_table["release_date"].apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime("%Y-%m-%d"))

    zzz_facts_table = zzz_facts_table.drop("Unnamed: 0", axis = 1)
    zzz_facts_table.rename(columns={'gender': 'gender_id', 'faction': 'faction_id'}, inplace=True)

    # saving transformed data tables

    logging.info("Star schema tables generated. Saving tables...")

    zzz_gender_df.to_csv("temp/zzz_gender_df.csv")
    zzz_faction_df.to_csv("temp/zzz_faction_df.csv")
    zzz_facts_table.to_csv("temp/zzz_facts_table.csv")

    logging.info("Tables saved.")

def transform_ow_csv_into_tables() -> None:

    """
    This function gets the CSV file containing the extracted data of Overwatch 2 characters
    and converts it to a list of CSV files which follow the star schema. In this case, both gender 
    and region are dimensions.

    Parameters:
    None

    Returned value:
    None
    """

    logging.info("Generating star schema for character data from game: Overwatch 2...")

    logging.info("Creating dimension tables...")

    # A. Getting dimension tables

    ow_char_df = pd.read_csv("temp/overwatch_2_character_data.csv")
    ow_char_df = ow_char_df[ow_char_df["release_date"] != "TBA"] # Only active characters are valid

    ow_gender_df = ow_char_df.groupby(["gender"]).count().reset_index()
    ow_gender_df["gender_id"] = ow_gender_df.index
    ow_gender_df["gender_id"] = ow_gender_df["gender_id"].apply(lambda x: x + 1)
    ow_gender_df = ow_gender_df[["gender_id", "gender"]]

    ow_region_df = ow_char_df.groupby(["region"]).count().reset_index()
    ow_region_df["region_id"] = ow_region_df.index
    ow_region_df["region_id"] = ow_region_df["region_id"].apply(lambda x: x + 1)
    ow_region_df = ow_region_df[["region_id", "region"]]

    # B. Getting facts table

    logging.info("Creating facts table...")

    ow_facts_table = ow_char_df
    ow_facts_table.insert(0, "character_id", ow_facts_table.index + 1)

    gender_assign_func = lambda x : ow_gender_df[ow_gender_df["gender"] == x]["gender_id"].values[0]
    ow_facts_table["gender"] = ow_facts_table["gender"].apply(gender_assign_func)

    region_assign_func = lambda x : ow_region_df[ow_region_df["region"] == x]["region_id"].values[0]
    ow_facts_table["region"] = ow_facts_table["region"].apply(region_assign_func)

    ow_facts_table["release_date"] = ow_facts_table["release_date"].apply(lambda x: datetime.strptime(x, "%d %B %Y").strftime("%Y-%m-%d"))

    ow_facts_table = ow_facts_table.drop("Unnamed: 0", axis = 1)
    ow_facts_table.rename(columns={'gender': 'gender_id', 'region': 'region_id'}, inplace=True)

    # saving transformed data tables

    logging.info("Star schema tables generated. Saving tables...")

    ow_gender_df.to_csv("temp/ow_gender_df.csv")
    ow_region_df.to_csv("temp/ow_region_df.csv")
    ow_facts_table.to_csv("temp/ow_facts_table.csv")

    logging.info("Tables saved.")

#  ======================================== Loading functions ====================================== #

def load_wuwa_tables_to_db() -> None:

    """
    This function loads the CSV files to their respective tables in the PostgreSQL schema then deletes
    the CSV files.

    Parameters:
    None

    Returned value:
    None
    """

    with open(".admin/.host.txt", "r") as f:
        host = f.read().strip()
    with open(".admin/.passw.txt", "r") as f:
        passw = f.read().strip()

    # Connector settings
    config = {
        'host': host,
        'database': 'kuro_games_characters',
        'user': 'postgres',
        'password': passw,
        'port': '5432'
    }

    # Loading operation
    try:

        logging.info("Accessing database: kuro_games_characters...")

        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        logging.info("Creating tables for schema: wuthering_waves...")
        
        # Creating tables
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS wuthering_waves.gender_dim (
                       gender_id INT PRIMARY KEY,
                       gender TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS wuthering_waves.region_dim (
                       region_id INT PRIMARY KEY,
                       region TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS wuthering_waves.character_info (
                       character_id INT NOT NULL,
                       name TEXT NOT NULL,
                       gender_id INT NOT NULL,
                       region_id INT NOT NULL,
                       release_date DATE NOT NULL,
                       PRIMARY KEY(character_id),
                       CONSTRAINT fk_gender FOREIGN KEY(gender_id) REFERENCES wuthering_waves.gender_dim(gender_id),
                       CONSTRAINT fk_region FOREIGN KEY(region_id) REFERENCES wuthering_waves.region_dim(region_id)
                    )
        """)

        logging.info("Uploading Wuthering Waves character data to schema: wuthering_waves...")

        # Inserting data into created tables
        df = pd.read_csv("temp/wuwa_gender_df.csv")
        query = f"INSERT INTO wuthering_waves.gender_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        print(query)
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/wuwa_gender_df.csv")

        df = pd.read_csv("temp/wuwa_region_df.csv")
        query = f"INSERT INTO wuthering_waves.region_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/wuwa_region_df.csv")

        df = pd.read_csv("temp/wuwa_facts_table.csv")
        query = f"INSERT INTO wuthering_waves.character_info ({', '.join(df.columns[1:])}) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/wuwa_facts_table.csv")

        logging.info("Finished uploading data to schema.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def load_hoyo_tables_to_db() -> None:

    """
    This function loads the CSV files to their respective tables in the PostgreSQL schema then deletes
    the CSV files.

    Parameters:
    None

    Returned value:
    None
    """

    with open(".admin/.host.txt", "r") as f:
        host = f.read().strip()
    with open(".admin/.passw.txt", "r") as f:
        passw = f.read().strip()

    # Connector settings
    config = {
        'host': host,
        'database': 'hoyo_characters',
        'user': 'postgres',
        'password': passw,
        'port': '5432'
    }

    # Loading operation
    try:

        logging.info("Accessing database: hoyo_characters...")

        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        ### ------------------- I. Genshin Impact characters ------------------- ###
        
        logging.info("Creating tables for schema: genshin_impact...")

        # Creating tables
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS genshin_impact.gender_dim (
                       gender_id INT PRIMARY KEY,
                       gender TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS genshin_impact.region_dim (
                       region_id INT PRIMARY KEY,
                       region TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS genshin_impact.character_info (
                       character_id INT NOT NULL,
                       name TEXT NOT NULL,
                       gender_id INT NOT NULL,
                       region_id INT NOT NULL,
                       release_date DATE NOT NULL,
                       PRIMARY KEY(character_id),
                       CONSTRAINT fk_gender FOREIGN KEY(gender_id) REFERENCES genshin_impact.gender_dim(gender_id),
                       CONSTRAINT fk_region FOREIGN KEY(region_id) REFERENCES genshin_impact.region_dim(region_id)
                    )
        """)

        logging.info("Uploading Genshin Impact character data to schema: genshin_impact...")

        # Inserting data into created tables
        df = pd.read_csv("temp/genshin_gender_df.csv")
        query = f"INSERT INTO genshin_impact.gender_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        print(query)
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/genshin_gender_df.csv")

        df = pd.read_csv("temp/genshin_region_df.csv")
        query = f"INSERT INTO genshin_impact.region_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/genshin_region_df.csv")

        df = pd.read_csv("temp/genshin_facts_table.csv")
        query = f"INSERT INTO genshin_impact.character_info ({', '.join(df.columns[1:])}) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/genshin_facts_table.csv")

        logging.info("Finished uploading data to schema.")

        ### ----------------- II. Honkai: Star Rail characters ----------------- ###

        logging.info("Creating tables for schema: honkai_star_rail...")
        
        # Creating tables
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS honkai_star_rail.gender_dim (
                       gender_id INT PRIMARY KEY,
                       gender TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS honkai_star_rail.faction_dim (
                       faction_id INT PRIMARY KEY,
                       faction TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS honkai_star_rail.character_info (
                       character_id INT NOT NULL,
                       name TEXT NOT NULL,
                       gender_id INT NOT NULL,
                       faction_id INT NOT NULL,
                       release_date DATE NOT NULL,
                       PRIMARY KEY(character_id),
                       CONSTRAINT fk_gender FOREIGN KEY(gender_id) REFERENCES honkai_star_rail.gender_dim(gender_id),
                       CONSTRAINT fk_faction FOREIGN KEY(faction_id) REFERENCES honkai_star_rail.faction_dim(faction_id)
                    )
        """)

        logging.info("Uploading Honkai: Star Rail character data to schema: honkai_star_rail...")

        # Inserting data into created tables
        df = pd.read_csv("temp/hsr_gender_df.csv")
        query = f"INSERT INTO honkai_star_rail.gender_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        print(query)
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/hsr_gender_df.csv")

        df = pd.read_csv("temp/hsr_faction_df.csv")
        query = f"INSERT INTO honkai_star_rail.faction_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/hsr_faction_df.csv")

        df = pd.read_csv("temp/hsr_facts_table.csv")
        query = f"INSERT INTO honkai_star_rail.character_info ({', '.join(df.columns[1:])}) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/hsr_facts_table.csv")

        logging.info("Finished uploading data to schema.")

        ### ---------------- III. Zenless Zone Zero characters ----------------- ###

        logging.info("Creating tables for schema: zenless_zone_zero...")
        
        # Creating tables
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS zenless_zone_zero.gender_dim (
                       gender_id INT PRIMARY KEY,
                       gender TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS zenless_zone_zero.faction_dim (
                       faction_id INT PRIMARY KEY,
                       faction TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS zenless_zone_zero.character_info (
                       character_id INT NOT NULL,
                       name TEXT NOT NULL,
                       gender_id INT NOT NULL,
                       faction_id INT NOT NULL,
                       release_date DATE NOT NULL,
                       PRIMARY KEY(character_id),
                       CONSTRAINT fk_gender FOREIGN KEY(gender_id) REFERENCES zenless_zone_zero.gender_dim(gender_id),
                       CONSTRAINT fk_faction FOREIGN KEY(faction_id) REFERENCES zenless_zone_zero.faction_dim(faction_id)
                    )
        """)

        logging.info("Uploading Zenless Zone Zero character data to schema: zenless_zone_zero...")

        # Inserting data into created tables
        df = pd.read_csv("temp/zzz_gender_df.csv")
        query = f"INSERT INTO zenless_zone_zero.gender_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        print(query)
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/zzz_gender_df.csv")

        df = pd.read_csv("temp/zzz_faction_df.csv")
        query = f"INSERT INTO zenless_zone_zero.faction_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/zzz_faction_df.csv")

        df = pd.read_csv("temp/zzz_facts_table.csv")
        query = f"INSERT INTO zenless_zone_zero.character_info ({', '.join(df.columns[1:])}) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/zzz_facts_table.csv")

        logging.info("Finished uploading data to schema.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def load_ow_tables_to_db() -> None:

    """
    This function loads the CSV files to their respective tables in the PostgreSQL schema then deletes
    the CSV files.

    Parameters:
    None

    Returned value:
    None
    """

    with open(".admin/.host.txt", "r") as f:
        host = f.read().strip()
    with open(".admin/.passw.txt", "r") as f:
        passw = f.read().strip()

    # Connector settings
    config = {
        'host': host,
        'database': 'blizzard_characters',
        'user': 'postgres',
        'password': passw,
        'port': '5432'
    }

    # Loading operation
    try:

        logging.info("Accessing database: blizzard_characters...")

        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        logging.info("Creating tables for schema: overwatch_2...")
        
        # Creating tables
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS overwatch_2.gender_dim (
                       gender_id INT PRIMARY KEY,
                       gender TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS overwatch_2.region_dim (
                       region_id INT PRIMARY KEY,
                       region TEXT
                    )
        """)
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS overwatch_2.character_info (
                       character_id INT NOT NULL,
                       name TEXT NOT NULL,
                       gender_id INT NOT NULL,
                       region_id INT NOT NULL,
                       release_date DATE NOT NULL,
                       PRIMARY KEY(character_id),
                       CONSTRAINT fk_gender FOREIGN KEY(gender_id) REFERENCES overwatch_2.gender_dim(gender_id),
                       CONSTRAINT fk_region FOREIGN KEY(region_id) REFERENCES overwatch_2.region_dim(region_id)
                    )
        """)

        logging.info("Uploading Overwatch 2 character data to schema: overwatch_2...")

        # Inserting data into created tables
        df = pd.read_csv("temp/ow_gender_df.csv")
        query = f"INSERT INTO overwatch_2.gender_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        print(query)
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/ow_gender_df.csv")

        df = pd.read_csv("temp/ow_region_df.csv")
        query = f"INSERT INTO overwatch_2.region_dim ({', '.join(df.columns[1:])}) VALUES (%s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/ow_region_df.csv")

        df = pd.read_csv("temp/ow_facts_table.csv")
        query = f"INSERT INTO overwatch_2.character_info ({', '.join(df.columns[1:])}) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(query, [tuple(x)[1:] for x in df.values])
        conn.commit()
        os.remove("temp/ow_facts_table.csv")

        logging.info("Finished uploading data to schema.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


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


#  ========================================== Main program ========================================= #

# setting arguments
default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# changing root directory
os.chdir("/mnt/c/Users/chris/Documents/GitHub Projects/auto_etl_sql")

# initialising DAG
with DAG(
    'etl_game_char_pipeline',
    default_args=default_args,
    description='A complete ETL pipeline with Airflow used to load data of characters from 5 games to three databases',
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:
    
    ## creating pipeline tasks

    extract_task = PythonOperator(
        task_id='extract_data_scraping',
        python_callable=extract_main_scraper,
        provide_context=True,
    )

    clean_task = PythonOperator(
        task_id='clean_scraped_data',
        python_callable=transform_fix_scraped_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_character_data',
        python_callable=transform_main_convert_to_star_schema,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_character_data',
        python_callable=load_main_tables_to_db,
        provide_context=True,
    )

    # run pipeline
    extract_task >> clean_task >> transform_task >> load_task

