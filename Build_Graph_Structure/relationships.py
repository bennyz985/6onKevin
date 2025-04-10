

from neo4j import GraphDatabase
import csv
import time
import json

uri = "bolt://localhost:7687"
username = "neo4j"
password = "SixKevin"
driver = GraphDatabase.driver(uri, auth=(username, password))

def create_played_role_relationships_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MATCH (p:Person {nconst: row['nconst']})
    MATCH (m:Movie {tconst: row['tconst']})
    CREATE (p)-[:PLAYED_ROLE_IN {
        job: CASE
            WHEN row['category'] = 'director' THEN 'director'
            WHEN row['category'] = 'writer' THEN 'writer'
            WHEN row['job'] = '\\N' THEN null
            ELSE row['job']
        END,
        characters: CASE
            WHEN row['category'] IN ['actor', 'actress'] AND row['characters'] = '\\N' THEN ['Undefined']
            WHEN row['category'] = 'self' AND (row['characters'] IS NOT NULL AND row['characters'] <> '\\N' AND row['characters'] <> '"Self"') THEN apoc.convert.fromJsonList(row['characters'])
            WHEN row['characters'] IS NOT NULL AND row['characters'] <> '\\N' THEN apoc.convert.fromJsonList(row['characters'])
            ELSE null
        END
    }]->(m)
    """
    tx.run(query, batch=batch)

def create_played_role_relationship_indexes(tx):
    tx.run("CREATE OR REPLACE INDEX played_role_person_nconst FOR (p:Person)-[r:PLAYED_ROLE_IN]-(m:Movie) ON (p.nconst)")
    tx.run("CREATE OR REPLACE INDEX played_role_movie_tconst FOR (p:Person)-[r:PLAYED_ROLE_IN]-(m:Movie) ON (m.tconst)")
    print("Indexes created or replaced for PLAYED_ROLE_IN relationships based on Person.nconst and Movie.tconst")

file_path = "/Users/benzuckerman/Documents/GitHub/7onKevin/Data_Files/princpals.tsv"
batch_size = 10000
report_interval = 100000
total_processed = 0
start_time = time.time()

with open(file_path, 'r', encoding='utf-8') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    batch = []
    with driver.session() as session:
        for row in reader:
            category = row['category']
            characters_str = row['characters'].strip() 

            if category in ['actor', 'actress', 'director', 'writer'] or (category == 'self' and characters_str != '\\N' and characters_str != '"Self"'):
                batch.append(row)
                if len(batch) >= batch_size:
                    session.execute_write(create_played_role_relationships_batch, batch)
                    total_processed += len(batch)
                    batch = []

                    if total_processed % report_interval == 0:
                        elapsed_time = time.time() - start_time
                        print(f"Processed {total_processed} principals and attempted to create PLAYED_ROLE_IN relationships in {elapsed_time:.2f} seconds (only if Person and Movie nodes exist).")

        if batch:
            session.execute_write(create_played_role_relationships_batch, batch)
            total_processed += len(batch)
            elapsed_time = time.time() - start_time
            print(f"Processed a final {len(batch)} principals and attempted to create PLAYED_ROLE_IN relationships in {elapsed_time:.2f} seconds (only if Person and Movie nodes exist).")

        session.execute_write(create_played_role_relationship_indexes)
    driver.close
    print("Driver Closed.")
