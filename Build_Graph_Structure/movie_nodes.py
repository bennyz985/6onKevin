from neo4j import GraphDatabase
import csv
import time
from dotenv import load_dotenv
import os
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")
driver = None
imdb_data_dir = os.getenv("DATA_DIRECTORY")
file_path = os.path.join(imdb_data_dir, "titles.tsv")

#check db connection
try:
    driver = GraphDatabase.driver(uri, auth=(username, password))
except Exception as e:
    logging.critical(f"Failed to connect to Neo4j: {e}")
    sys.exit(1)

def create_movie_index(tx):
    try:
        tx.run("CREATE INDEX movie_tconst IF NOT EXISTS FOR (m:Movie) ON (m.tconst)")
        logging.info("Attempted to create index for Movie.tconst (IF NOT EXISTS).")
    except Exception as e:
        logging.error(f"Error creating index for Movie.tconst: {e}")
        raise
# need person to build index so other person nodes can be created efficiently
def create_dummy_movie_node(tx):
    query = """
    CREATE (m:Movie {tconst: 'tt_dummy_index'})
    """
    try:
        tx.run(query)
        logging.info("Created dummy Movie node for indexing.")
    except Exception as e:
        logging.error(f"Error creating dummy movie node: {e}")
        raise
# unwind data off tsv file
def create_movie_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MERGE (m:Movie {tconst: row['tconst']})
    SET m += {
        titleType: CASE WHEN row['titleType'] <> '\\N' THEN row['titleType'] ELSE null END,
        primaryTitle: CASE WHEN row['primaryTitle'] <> '\\N' THEN row['primaryTitle'] ELSE null END,
        originalTitle: CASE WHEN row['originalTitle'] <> '\\N' THEN row['originalTitle'] ELSE null END,
        isAdult: CASE WHEN row['isAdult'] <> '\\N' THEN toInteger(row['isAdult']) ELSE null END,
        startYear: CASE WHEN row['startYear'] <> '\\N' THEN toInteger(row['startYear']) ELSE null END,
        endYear: CASE WHEN row['endYear'] <> '\\N' THEN toInteger(row['endYear']) ELSE null END,
        runtimeMinutes: CASE WHEN row['runtimeMinutes'] <> '\\N' THEN toInteger(row['runtimeMinutes']) ELSE null END,
        genres: CASE WHEN row['genres'] <> '\\N' THEN split(row['genres'], ',') ELSE null END
    }
    """
    try:
        tx.run(query, batch=batch)
    except Exception as e:
        logging.error(f"Error executing movie batch: {e}. Batch data (first 5): {batch[:5]}")

def process_movie_data(driver, file_path, batch_size, report_interval):
    total_processed = 0
    start_time = time.time()
    elapsed_total_time = 0.0
    logging.info(f"Starting processing of movie data from: {file_path}")


    try:
        with driver.session() as session:
            session.execute_write(create_dummy_movie_node)
            session.execute_write(create_movie_index)
            logging.info("Dummy node and index creation completed before data loading.")
    except Exception as e:
        logging.error(f"Error during initial dummy node and index creation: {e}")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            batch = []

            for i, row in enumerate(reader):
                try:
                    if row['titleType'] == 'movie':
                        batch.append(row)
                        if len(batch) >= batch_size:
                            try:
                                with driver.session() as session:
                                    with session.begin_transaction() as tx:
                                        create_movie_batch(tx, batch)
                                        tx.commit()
                                total_processed += len(batch)
                                batch = []
                                if total_processed % report_interval == 0:
                                    elapsed_time = time.time() - start_time
                                    logging.info(f"Processed and created {total_processed} movie/tvSeries records in {elapsed_time:.2f} seconds.")
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
                    with session.begin_transaction() as tx:
                        create_movie_batch(tx, batch)
                        tx.commit()
                total_processed += len(batch)
                elapsed_time = time.time() - start_time
                logging.info(f"Processed and created a final {len(batch)} movie/tvSeries records in {elapsed_total_time:.2f} seconds.")
            except Exception as e:
                logging.error(f"Error processing final batch: {e}")

        elapsed_total_time = time.time() - start_time
        logging.info(f"Total of {total_processed} movie/tvSeries records processed in {elapsed_total_time:.2f} seconds.")

        try:
            with driver.session() as session:
                session.execute_write(lambda tx: tx.run("MATCH (m:Movie {tconst: 'tt_dummy_index'}) DELETE m"))
                logging.info("Dummy movie node cleaned up.")
        except Exception as e:
            logging.warning(f"Error cleaning up dummy movie node: {e}")

    except FileNotFoundError as e:
        logging.error(f"Error: IMDB data file not found at: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during movie data processing: {e}")

    finally:
        if driver:
            driver.close()
            logging.info("Neo4j driver closed.")

if __name__ == "__main__":
    batch_size = 10000
    report_interval = 100000
    if driver:
        process_movie_data(driver, file_path, batch_size, report_interval)
    else:logging.critical(f"Driver Failure")
    sys.exit(1)
    