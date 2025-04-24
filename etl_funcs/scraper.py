from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import pandas as pd
import logging
import random
import time
import sys

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