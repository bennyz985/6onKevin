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
driver = GraphDatabase.driver(uri, auth=(username, password))
imdb_data_dir = os.getenv("DATA_DIRECTORY")
file_path = os.path.join(imdb_data_dir, "titles.tsv")

def create_movie_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MERGE (m:Movie {
        tconst: row['tconst'],
        titleType: row['titleType'],
        primaryTitle: row['primaryTitle'],
        originalTitle: row['originalTitle'],
        isAdult: toInteger(row['isAdult']),
        startYear: CASE WHEN row['startYear'] = '\\N' THEN null ELSE toInteger(row['startYear']) END,
        endYear: CASE WHEN row['endYear'] = '\\N' THEN null ELSE toInteger(row['endYear']) END,
        runtimeMinutes: CASE WHEN row['runtimeMinutes'] = '\\N' THEN null ELSE toInteger(row['runtimeMinutes']) END,
        genres: split(row['genres'], ',')
    })
    """
    try:
        tx.run(query, batch=batch)
    except Exception as e:
        logging.error(f"Error executing movie batch: {e}. Batch data (first 5): {batch[:5]}")

def create_movie_indexes(tx):
    try:
        tx.run("CREATE OR REPLACE INDEX movie_tconst FOR (m:Movie) ON (m.tconst)")
        logging.info("Index created or replaced for Movie.tconst")
    except Exception as e:
        logging.error(f"Error creating/replacing index for Movie.tconst: {e}")
        raise  # Problems creating an index should bomb the whole process


def process_movie_data(driver, file_path, batch_size, report_interval):
    total_processed = 0
    start_time = time.time()
    logging.info(f"Starting processing of movie data from: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            batch = []

            with driver.session() as session:
                for i, row in enumerate(reader):
                    try:
                        if row['titleType'] in ['movie','tvSeries']:
                            batch.append(row)
                            if len(batch) >= batch_size:
                                session.execute_write(create_movie_batch, batch)
                                total_processed += len(batch)
                                batch = []

                                if total_processed % report_interval == 0:
                                    elapsed_time = time.time() - start_time
                                    logging.info(f"Processed and created {total_processed} movie/tvSeries records in {elapsed_time:.2f} seconds.")
                    except ValueError as ve:
                        logging.warning(f"Skipping row {i+1} due to data conversion error: {ve}. Row data: {row}")
                    except Exception as e:
                        logging.error(f"Error processing row {i+1}: {e}. Row data: {row}")

            if batch:
                session.execute_write(create_movie_batch, batch)
                total_processed += len(batch)
                elapsed_time = time.time() - start_time
                logging.info(f"Processed and created a final {len(batch)} movie/tvSeries records in {elapsed_total_time:.2f} seconds.")

            session.execute_write(create_movie_indexes)


        if batch:
            session.execute_write(create_movie_batch, batch)
            total_processed += len(batch)
            elapsed_time = time.time() - start_time
            print(f"Processed and created a final {len(batch)} movie/tvSeries records in {elapsed_time:.2f} seconds.")

        session.execute_write(create_movie_indexes)
    except FileNotFoundError:
        logging.error(f"Error: IMDB data file not found at: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during movie data processing: {e}")
        sys.exit(1)
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j driver closed.")

if __name__ == "__main__":
    batch_size = 10000
    report_interval = 100000
    process_movie_data(driver, file_path, batch_size, report_interval)