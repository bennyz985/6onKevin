# building implied relationships is expensive, running this file takes a couple minutes to complete 

from neo4j import GraphDatabase
import csv
import time
from dotenv import load_dotenv
import os
import json
import logging
import sys

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")
driver = None
imdb_data_dir = os.getenv("DATA_DIRECTORY")
file_path = os.path.join(imdb_data_dir, "principals.tsv")

# check database connection
try:
    driver = GraphDatabase.driver(uri, auth=(username, password))
except Exception as e:
    logging.critical(f"Failed to connect to Neo4j: {e}")
    sys.exit(1)

def create_played_role_relationships_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MATCH (p:Person {nconst: row['nconst']})
    MATCH (m:Movie {tconst: row['tconst']})
    MERGE (p)-[r:PLAYED_ROLE_IN {category: row['category'], characters: row['characters']}]->(m)
    ON CREATE SET
        r.job = CASE
            WHEN row['category'] IN ['director', 'writer'] AND row['job'] <> '\\\\N' THEN row['job']
            ELSE null
        END,
        r.characters = CASE
            WHEN row['characters'] = '\\\\N' THEN 'Undefined'
            WHEN row['characters'] IS NOT NULL AND row['characters'] <> '\\\\N' THEN row['characters']
            ELSE null
        END
    ON MATCH SET
        r.job = CASE
            WHEN row['category'] IN ['director', 'writer'] AND row['job'] <> '\\\\N' THEN row['job']
            ELSE r.job
        END,
        r.characters = CASE
            WHEN row['characters'] = '\\\\N' THEN 'Undefined'
            WHEN row['characters'] IS NOT NULL AND row['characters'] <> '\\\\N' THEN row['characters']
            ELSE r.characters
        END
    WITH r
    WHERE r.job IS NOT NULL OR r.characters IS NOT NULL
    RETURN r
    """
    try:
        tx.run(query, batch=batch)
    except Exception as e:
        logging.error(f"Error creating/merging played role relationships batch: {e}. Batch data (first 5): {batch[:5]}")
        raise
# Not sure if indexing relationships is actually important, but might as well
def create_played_role_relationship_indexes(tx):
    try:
        tx.run("CREATE INDEX played_role_job IF NOT EXISTS FOR ()-[r:PLAYED_ROLE_IN]-() ON (r.job)")
        tx.run("CREATE INDEX played_role_characters IF NOT EXISTS FOR ()-[r:PLAYED_ROLE_IN]-() ON (r.characters)")
        logging.info("Indexes created or checked for PLAYED_ROLE_IN relationships on properties 'job' and 'characters'.")
    except Exception as e:
        logging.error(f'Error creating relationship indexes: {e}')
        raise


def process_played_role_relationships(driver, file_path, batch_size, report_interval):
    total_processed = 0
    start_time = time.time()
    logging.info("Starting processing of played role relationships.")

    try:
        with open(file_path, 'r', encoding='utf-8') as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            batch = []

            for i, row in enumerate(reader):
                try:
                    category = row['category']
                    characters_str = row['characters'].strip()

                    if category in ['actor', 'actress', 'director', 'writer'] or (category == 'self' and characters_str != '\\N' and characters_str != '"Self"'):
                        batch.append(row)
                        if len(batch) >= batch_size:
                            try:
                                with driver.session() as session:
                                    session.execute_write(create_played_role_relationships_batch, batch)
                                total_processed += len(batch)
                                batch = []
                                if total_processed % report_interval == 0:
                                    elapsed_time = time.time() - start_time
                                    logging.info(f"Processed {total_processed} principals and created PLAYED_ROLE_IN relationships in {elapsed_time:.2f} seconds")
                            except Exception as e:
                                logging.error(f"Error processing batch: {e}")
                                break 
                except ValueError as ve:
                    logging.warning(f"Skipping row {i+1} due to data conversion error: {ve}. Row data: {row}")
                except Exception as e:
                    logging.error(f"Error processing row {i+1}: {e}. Row data: {row}")

            if batch:
                try:
                    with driver.session() as session:
                        session.execute_write(create_played_role_relationships_batch, batch)
                    total_processed += len(batch)
                    elapsed_time = time.time() - start_time
                    logging.info(f"Processed a final {len(batch)} principals and created PLAYED_ROLE_IN relationships in {elapsed_time:.2f} seconds")
                except Exception as e:
                    logging.error(f"Error processing final batch: {e}")

            try:
                with driver.session() as session:
                    session.execute_write(create_played_role_relationship_indexes)
                    logging.info("Played role relationship index creation process completed.")
            except Exception as e:
                logging.error(f"Error creating played role relationship indexes: {e}")

            elapsed_total_time = time.time() - start_time
            logging.info(f"Total of {total_processed} principals processed and PLAYED_ROLE_IN relationships attempted in {elapsed_total_time:.2f} seconds.")

    except FileNotFoundError:
        logging.error(f"Error: Principals data file not found at: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during played role relationship processing: {e}")
        sys.exit(1)
    finally:
       pass # default pass because moving session management inside the main execution

if __name__ == "__main__":
    batch_size = 10000
    report_interval = 100000
    try:
        process_played_role_relationships(driver, file_path, batch_size, report_interval)
    finally:
        driver.close
        logging.info("Neo4j driver closed.")