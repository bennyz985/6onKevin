from neo4j import GraphDatabase
import csv
import time
from dotenv import load_dotenv
import os
import logging
import sys

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")
imdb_data_dir = os.getenv("DATA_DIRECTORY")
file_path_names = os.path.join(imdb_data_dir, "names.tsv")
file_path_principals = os.path.join(imdb_data_dir, "principals.tsv")
driver = None

try:
    driver = GraphDatabase.driver(uri, auth=(username, password))
except Exception as e:
    logging.critical(f"Failed to connect to Neo4j: {e}")
    sys.exit(1)

def get_existing_movie_tconsts(tx):
    query = """
    MATCH (m:Movie)
    RETURN DISTINCT m.tconst AS tconst
    """
    result = tx.run(query)
    return set(record['tconst'] for record in result)

def create_person_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MERGE (p:Person {nconst: row['nconst']})
    SET p += {
        primaryName: CASE WHEN row['primaryName'] <> '\\N' THEN row['primaryName'] ELSE null END,
        birthYear: CASE WHEN row['birthYear'] <> '\\N' THEN toInteger(row['birthYear']) ELSE null END,
        deathYear: CASE WHEN row['deathYear'] <> '\\N' THEN toInteger(row['deathYear']) ELSE null END,
        primaryProfession: CASE WHEN row ['primaryProfession'] <> '\\N' THEN split(row['primaryProfession'], ',') ELSE null END,
        knownForTitles: CASE WHEN row['knownForTitles'] <> '\\N' THEN  split(row['knownForTitles'], ',') ELSE null END
    }"""
    try:
        result = tx.run(query, batch=batch)
    except Exception as e:
        logging.error(f"Error executing movie batch: {e}. Batch data (first 5): {batch[:5]}")

def create_single_person(tx):
    query = """
    CREATE (p:Person {nconst: 'temp_nconst_for_index', primaryName: 'Temp Name'})
    """
    try:
        tx.run(query)
    except Exception as e:
        logging.error(f"Error creating temporary Person node for index: {e}")

def create_person_indexes(tx):
    try:
        tx.run("CREATE INDEX person_nconst IF NOT EXISTS FOR (p:Person) ON (p.nconst)")
        logging.info("Index created or allready exists for Person.nconst")
    except Exception as e:
        logging.error(f"Error creating index for Person.nconst: {e}")
        raise  # Raise the error as index creation is important

def process_person_data(driver, file_path_names, file_path_principals, batch_size, report_interval):
    total_processed = 0
    updated_count = 0
    start_time = time.time()
    logging.info("Starting processing of person data.")

    try:
        with driver.session() as session:
            logging.info("Fetching existing tconsts from Movie nodes...")
            existing_movie_tconsts = session.execute_read(get_existing_movie_tconsts)
            logging.info(f"Found {len(existing_movie_tconsts)} unique tconsts in Movie nodes.")

            relevant_principals_nconsts = set()
            try:
                with open(file_path_principals, 'r', encoding='utf-8') as tsvfile:
                    reader = csv.DictReader(tsvfile, delimiter='\t')
                    for row in reader:
                        category = row['category']
                        characters = row['characters']
                        tconst = row['tconst']

                        if tconst in existing_movie_tconsts:
                            if category in ['actor', 'actress', 'director'] or (category == 'self' and characters not in ('\\N', '"Self"')): # only create nodes for actors actresses and directors (sometimes actor/actress are listed in a category called 'self')
                                relevant_principals_nconsts.add(row['nconst'])
                logging.info(f"Found {len(relevant_principals_nconsts)} unique nconsts in principals.tsv associated with existing movies and relevant categories.")
                logging.debug(f"Sample relevant nconsts: {list(relevant_principals_nconsts)[:5]}")
            except FileNotFoundError:
                logging.error(f"Error: Principals data file not found at: {file_path_principals}")
                sys.exit(1)  # Stop processing if a critical file is missing
            except Exception as e:
                logging.error(f"Error reading principals data: {e}")
                return

            logging.info("Creating a temporary Person node for index creation...")
            session.execute_write(create_single_person)
            logging.info("Temporary Person node created.")

            logging.info("Creating index on Person.nconst...")
            session.execute_write(create_person_indexes)

            logging.info("Processing names.tsv to create Person nodes...")
            try:
                with open(file_path_names, 'r', encoding='utf-8') as tsvfile:
                    reader = csv.DictReader(tsvfile, delimiter='\t')
                    batch = []
                    logging.info("Successfully opened and created reader for names.tsv")
                    for i, row in enumerate(reader):
                        nconst = row['nconst']
                        try:
                            if nconst in relevant_principals_nconsts:
                                batch.append(row)
                                if len(batch) >= batch_size:
                                    batch_start_time = time.time()
                                    with session.begin_transaction() as tx:
                                        counts = create_person_batch(tx, batch)
                                        tx.commit()
                                        if counts:
                                            total_processed += counts["processed"]
                                            updated_count += counts["properties_set"]
                                            logging.info(f"Batch {total_processed // batch_size} processed in {time.time() - batch_start_time:.2f} seconds. Processed: {counts['processed']}, Properties Set: {counts['properties_set']}.")
                                    batch = []
                        except Exception as e:
                            logging.error(f"Error processing row {i+1} with nconst '{nconst}': {e}. Row data: {row}")

                        if (i + 1) % report_interval == 0:
                            elapsed_time = time.time() - start_time
                            logging.info(f"Processed {i + 1} lines from names.tsv in {elapsed_time:.2f} seconds.")

                    if batch:
                        logging.info("Processing final batch...")
                        batch_start_time = time.time()
                        with session.begin_transaction() as tx:
                            counts = create_person_batch(tx, batch)
                            tx.commit()
                            if counts:
                                total_processed += counts["processed"]
                                updated_count += counts["properties_set"]
                                logging.info(f"Final batch of {len(batch)} processed in {time.time() - batch_start_time:.2f} seconds. Processed: {counts['processed']}, Properties Set: {counts['properties_set']}.")
                        total_processed += len(batch)
            except FileNotFoundError:
                logging.error(f"Error: Names data file not found at: {file_path_names}")
                sys.exit(1)
            except Exception as e:
                logging.error(f"Error reading names data: {e}")
                sys.exit(1)

            session.execute_write(create_person_indexes)
            logging.info("Index check completed.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during person data processing: {e}")
    finally:
       pass # default pass because moving session management inside the main execution

if __name__ == "__main__":
    batch_size = 10000
    report_interval = 100000
    try:
        process_person_data(driver, file_path_names, file_path_principals, batch_size, report_interval)
    finally:
        driver.close
        logging.info("Neo4j driver closed.")