import psycopg2
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()

#  ======================================= Loading functions ======================================= #

def load_wuwa_tables_to_db() -> None:

    """
    This function loads the CSV files to their respective tables in the PostgreSQL schema then deletes
    the CSV files.

    Parameters:
    None

    Returned value:
    None
    """

    # Connector settings
    config = {
        'host': 'localhost',
        'database': 'kuro_games_characters',
        'user': 'postgres',
        'password': os.getenv("POSTGRES_MASTER_PASSW"),
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

    # Connector settings
    config = {
        'host': 'localhost',
        'database': 'hoyo_characters',
        'user': 'postgres',
        'password': os.getenv("POSTGRES_MASTER_PASSW"),
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

    # Connector settings
    config = {
        'host': 'localhost',
        'database': 'blizzard_characters',
        'user': 'postgres',
        'password': os.getenv("POSTGRES_MASTER_PASSW"),
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
