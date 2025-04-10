from neo4j import GraphDatabase
import csv
import time
import json

uri = "bolt://localhost:7687"
username = "neo4j"
password = "SixKevin"
driver = GraphDatabase.driver(uri, auth=(username, password))

def create_role_nodes_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MATCH (m:Movie {tconst: row['tconst']})
    CREATE (r:Role {
        tconst: row['tconst'],
        nconst: row['nconst'],
        category: row['category'],
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
    })
    """
    tx.run(query, batch=batch)

def create_role_indexes(tx):
    tx.run("CREATE INDEX role_tconst FOR (r:Role) ON (r.tconst)")
    tx.run("CREATE INDEX role_nconst FOR (r:Role) ON (r.nconst)")
    tx.run("CREATE INDEX role_tconst_nconst FOR (r:Role) ON (r.nconst, r.tconst)")
    print("Indexes created for Role.tconst and Role.nconst")

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
            characters_str = row['characters'].strip()  #remove whitespace

            if category in ['actor', 'actress', 'director', 'writer'] or (category == 'self' and characters_str != '\\N' and characters_str != '"Self"'):
                batch.append(row)
                if len(batch) >= batch_size:
                    session.execute_write(create_role_nodes_batch, batch)
                    total_processed += len(batch)
                    batch = []

                    if total_processed % report_interval == 0:
                        elapsed_time = time.time() - start_time
                        print(f"Processed and attempted to create {total_processed} Role nodes in {elapsed_time:.2f} seconds (only if Title nodes exists).")

        if batch:
            session.execute_write(create_role_nodes_batch, batch)
            total_processed += len(batch)
            elapsed_time = time.time() - start_time
            print(f"Processed and attempted to create a final {len(batch)} Role nodes in {elapsed_total_time:.2f} seconds (only if Title nodes exists).")

        session.execute_write(create_role_indexes)

elapsed_total_time = time.time() - start_time
print(f"Total of {total_processed} Role nodes processed (attempted creation only if Title nodes exists) in {elapsed_total_time:.2f} seconds.")
driver.close()
print("Driver closed.")
