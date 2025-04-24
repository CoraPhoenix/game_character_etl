import streamlit as st
import psycopg2
import pandas as pd
import time
import warnings

warnings.filterwarnings("ignore")

def main() -> None:

    # variables
    database = None
    if "query" not in st.session_state:
        st.session_state.query = ""
    example_query = None

    # data structures
    db_dict = {"blizzard_characters" : ("overwatch_2",),
               "hoyo_characters" : ("genshin_impact", "honkai_star_rail", "zenless_zone_zero"),
               "kuro_games_characters" : ("wuthering_waves",)}
    schema_dict = {"genshin_impact" : "character_info | gender_dim | region_dim",
                   "honkai_star_rail" : "character_info | faction_dim | gender_dim",
                   "overwatch_2" : "character_info | gender_dim | region_dim",
                   "wuthering_waves" : "character_info | gender_dim | region_dim",
                   "zenless_zone_zero" : "character_info | faction_dim | gender_dim"}

    st.title("Game Character Data Playground")

    st.text("Welcome to this comfy and friendly playground, where you can have fun using your SQL skills. \
            Here you can type in the query box or click on a predefined one to get the information in the data tables. Pick a database to start:")
    
    # columns
    data_col, query_col = st.columns([1, 2])

    with data_col:
        database = st.selectbox("Pick a database:", ("blizzard_characters", "hoyo_characters", "kuro_games_characters"))

        if database:
            st.write("Example queries:")

            if st.button("Get number of characters per gender"):
                example_query = f"""SELECT gd.gender, COUNT(*) AS gender_count 
                                    FROM {db_dict[database][0]}.character_info ci
                                    JOIN {db_dict[database][0]}.gender_dim gd ON ci.gender_id = gd.gender_id
                                    GROUP BY gd.gender_id"""
            if st.button("Get number of characters per region or faction"):
                example_query = f"""SELECT rfd.region, COUNT(*) AS region_count 
                                    FROM {db_dict[database][0]}.character_info ci
                                    JOIN {db_dict[database][0]}.region_dim rfd ON ci.region_id = rfd.region_id
                                    GROUP BY rfd.region_id
                                    ORDER BY region_count DESC"""
            if st.button("Get total number of characters"):
                example_query = f"""SELECT COUNT(*) AS character_number 
                                    FROM {db_dict[database][0]}.character_info"""
    with query_col:
        st.write("Available tables:")
        if database:
            list_col, button_col = st.columns([2, 1])
            query_text = st.text_area("Insert your SQL query here:", value=example_query, height=400)
            if query_text:
                st.session_state.query = query_text

            time_placeholder = st.empty()
            result_placeholder = st.empty()

            with list_col:
                schema = st.selectbox("Pick a schema:", db_dict[database])
                if schema:
                    st.write(schema_dict[schema])
            with button_col:

                if st.button("Execute query"):
                    
                    # helper function
                    def run_query(database : str, query : str) -> None:

                        with open(".admin/.passw.txt", "r") as f:
                            passw = f.read().strip()

                        config = {
                            'host': 'localhost',
                            'database': database,
                            'user': 'postgres',
                            'password': passw,
                            'port': '5432'
                        }

                        try:
                            conn = psycopg2.connect(**config)
                            start = time.time()
                            df = pd.read_sql_query(query, conn)
                            time_placeholder.success(f"Query executed successfully! Elapsed time: {time.time() - start} seconds.")
                            result_placeholder.write(df)
                        except (Exception, psycopg2.DatabaseError) as e:
                            result_placeholder.error(e)
                        finally:
                            if conn is not None:
                                conn.close()

                    run_query(database, st.session_state.query)

    

main()