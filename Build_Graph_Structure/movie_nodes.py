from neo4j import GraphDatabase
import csv
import time

uri = "bolt://localhost:7687"
username = "neo4j"
password = "SixKevin"
driver = GraphDatabase.driver(uri, auth=(username, password))

def create_movie_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    CREATE (m:Movie {
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
    tx.run(query, batch=batch)

def create_movie_indexes(tx):
    tx.run("CREATE INDEX movie_tconst FOR (m:Movie) ON (m.tconst)")
    print("Index created for Movie.tconst")

file_path = "/Users/benzuckerman/Documents/GitHub/7degrees-of-bacon/imdb_data_files/title.tsv"
batch_size = 10000
report_interval = 100000
total_processed = 0
start_time = time.time()

with open(file_path, 'r', encoding='utf-8') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    batch = []

    with driver.session() as session:
        for row in reader:
            if row['titleType'] in ['movie','tvSeries']:
                batch.append(row)
                if len(batch) >= batch_size:
                    session.execute_write(create_movie_batch, batch)
                    total_processed += len(batch)
                    batch = []

                    if total_processed % report_interval == 0:
                        elapsed_time = time.time() - start_time
                        print(f"Processed and created {total_processed} movie/tvSeries records in {elapsed_time:.2f} seconds.")

        if batch:
            session.execute_write(create_movie_batch, batch)
            total_processed += len(batch)
            elapsed_time = time.time() - start_time
            print(f"Processed and created a final {len(batch)} movie/tvSeries records in {elapsed_time:.2f} seconds.")

        session.execute_write(create_movie_indexes)

elapsed_total_time = time.time() - start_time
print(f"Total of {total_processed} movie/tvSeries records processed and created in {elapsed_total_time:.2f} seconds.")
driver.close()
print("Driver closed.")