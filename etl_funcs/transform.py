import pandas as pd
from datetime import datetime
import logging

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

    ow_facts_table["release_date"] = ow_facts_table["release_date"].apply(lambda x: datetime.strptime(x, "%d-%b-%y").strftime("%Y-%m-%d"))

    ow_facts_table = ow_facts_table.drop("Unnamed: 0", axis = 1)
    ow_facts_table.rename(columns={'gender': 'gender_id', 'region': 'region_id'}, inplace=True)

    # saving transformed data tables

    logging.info("Star schema tables generated. Saving tables...")

    ow_gender_df.to_csv("temp/ow_gender_df.csv")
    ow_region_df.to_csv("temp/ow_region_df.csv")
    ow_facts_table.to_csv("temp/ow_facts_table.csv")

    logging.info("Tables saved.")
