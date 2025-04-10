
from neo4j import GraphDatabase
import csv
import time

uri = "bolt://localhost:7687"
username = "neo4j"
password = "SixKevin"
driver = GraphDatabase.driver(uri, auth=(username, password))

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
    SET p.primaryName = row['primaryName'],
        p.birthYear = CASE WHEN row['birthYear'] = '\\N' THEN null ELSE toInteger(row['birthYear']) END,
        p.deathYear = CASE WHEN row['deathYear'] = '\\N' THEN null Else toInteger(row['deathYear']) END,
        p.primaryProfession = split(row['primaryProfession'], ','),
        p.knownForTitles = split(row['knownForTitles'], ',')
    """
    result = tx.run(query, batch=batch)
    summary = result.consume()
    properties_set = getattr(summary.counters, 'properties_set', 0)
    return {"processed": len(batch), "properties_set": properties_set}

def create_single_person(tx):
    query = """
    CREATE (p:Person {nconst: 'temp_nconst_for_index', primaryName: 'Temp Name'})
    """
    tx.run(query)

def create_person_indexes(tx):
    tx.run("CREATE INDEX person_nconst FOR (p:Person) ON (p.nconst)")
    print("Index created for Person.nconst")

file_path_names = "/Users/benzuckerman/Documents/GitHub/7onKevin/Data_Files/names.tsv"
file_path_principals = "/Users/benzuckerman/Documents/GitHub/7onKevin/Data_Files/princpals.tsv"
batch_size = 5000
report_interval = 20000
total_processed = 0
created_count = 0
updated_count = 0
start_time = time.time()

with driver.session() as session:
    print("Fetching existing tconsts from Movie nodes...")
    existing_movie_tconsts = session.execute_read(get_existing_movie_tconsts)
    print(f"Found {len(existing_movie_tconsts)} unique tconsts in Movie nodes.")

    # Identify relevant nconsts from principals.tsv associated with existing movies
    relevant_principals_nconsts = set()
    with open(file_path_principals, 'r', encoding='utf-8') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            category = row['category']
            characters = row['characters']
            tconst = row['tconst']

            if tconst in existing_movie_tconsts:
                if category in ['actor', 'actress', 'director'] or (category == 'self' and characters not in ('\\N', '"Self"')):
                    relevant_principals_nconsts.add(row['nconst'])
    print(f"Found {len(relevant_principals_nconsts)} unique nconsts in principals.tsv associated with existing movies and relevant categories.")
    print("Sample nconsts from relevant_principals_nconsts:", list(relevant_principals_nconsts)[:10])

    print("Creating a temporary Person node for index creation...")
    session.execute_write(create_single_person)
    print("Temporary Person node created.")

    print("Creating index on Person.nconst...")
    try:
        session.execute_write(create_person_indexes)
    except Exception as e:
        print(f"Error during index creation: {e}")

    print("Processing names.tsv to create Person nodes...")
    with open(file_path_names, 'r', encoding='utf-8') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        batch = []
        print("Successfully opened and created reader for names.tsv")
        for i, row in enumerate(reader):
            nconst = row['nconst']
            try:
                if nconst == 'your_chosen_nconst_for_profiling':
                    profile_start_time = time.time()
                    with session.begin_transaction() as tx:
                        query = """
                        MERGE (p:Person {nconst: $nconst})
                        SET p.primaryName = $primaryName,
                            p.birthYear = CASE WHEN $birthYear = '\\N' THEN null ELSE toInteger($birthYear) END,
                            p.deathYear = CASE WHEN $deathYear = '\\N' THEN null Else toInteger($deathYear) END,
                            p.primaryProfession = split($primaryProfession, ','),
                            p.knownForTitles = split($knownForTitles, ',')
                        """
                        tx.run(query, row)
                        tx.commit()
                    profile_end_time = time.time()
                    profile_duration = profile_end_time - profile_start_time
                    print(f"Profiling MERGE for nconst '{nconst}': {profile_duration:.4f} seconds.")
                elif nconst in relevant_principals_nconsts:
                    batch.append(row)
                    if len(batch) >= batch_size:
                        batch_start_time = time.time()
                        with session.begin_transaction() as tx:
                            counts = create_person_batch(tx, batch)
                            tx.commit()
                            if counts:
                                total_processed += counts["processed"]
                                updated_count += counts["properties_set"]
                                print(f"Batch {total_processed // batch_size} processed in {time.time() - batch_start_time:.2f} seconds. Processed: {counts['processed']}, Properties Set: {counts['properties_set']}.")
                        batch = []
            except Exception as e:
                print(f"Error processing row {i+1} with nconst '{nconst}': {e}")
                break  # Stop processing the file if an error occurs

            if (i + 1) % report_interval == 0:
                elapsed_time = time.time() - start_time
                print(f"Processed {i + 1} lines from names.tsv in {elapsed_time:.2f} seconds.")

        if batch:
            print("Processing final batch...")
            try:
                batch_start_time = time.time()
                with session.begin_transaction() as tx:
                    counts = create_person_batch(tx, batch)
                    tx.commit()
                    if counts:
                        total_processed += counts["processed"]
                        updated_count += counts["properties_set"]
                        print(f"Final batch of {len(batch)} processed in {time.time() - batch_start_time:.2f} seconds. Processed: {counts['processed']}, Properties Set: {counts['properties_set']}.")
                total_processed += len(batch)
            except Exception as e:
                print(f"Error processing final batch: {e}")

    print("Starting final index check (should already exist)...")
    try:
        session.execute_write(create_person_indexes)
        print("Index check completed.")
    except Exception as e:
        print(f"Index check reported: {e}")

    print("Closing driver and printing final summary...")
    elapsed_total_time = time.time() - start_time
    print(f"Total of {total_processed} Person records processed in {elapsed_total_time:.2f} seconds. Total Processed: {total_processed}, Total Properties Set: {updated_count}.")
    driver.close()
    print("Driver closed.")