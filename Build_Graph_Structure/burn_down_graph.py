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

try:
    driver = GraphDatabase.driver(uri, auth=(username, password))
except Exception as e:
    logging.critical(f"Failed to connect to Neo4j: {e}")
    sys.exit(1)

def get_all_indexes(tx):
    result = tx.run("SHOW INDEXES")
    return [record for record in result]

def drop_index(tx, label, property_name):
    query = f"DROP INDEX ON (:{label}({property_name}))"
    tx.run(query)
    logging.info(f"Dropped index on :{label}({property_name})")

def drop_all_indexes(driver):
    logging.info("Starting to drop all indexes...")
    start_time = time.time()
    try:
        with driver.session() as session:
            indexes = session.execute_read(get_all_indexes)
            for index in indexes:
                if index['type'] == 'BTREE':
                    labelsOrTypes = index.get('labelsOrTypes')
                    properties = index.get('properties')
                    if labelsOrTypes and properties and len(labelsOrTypes) == 1 and len(properties) == 1:
                        label = labelsOrTypes[0]
                        property_name = properties[0]
                        session.execute_write(drop_index, label, property_name)
            elapsed_time = time.time() - start_time
            logging.info(f"Finished dropping indexes in {elapsed_time:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error dropping indexes: {e}")

def delete_graph_batch(tx, batch_size):
    query = """
    MATCH (n)
    WITH n LIMIT $batchSize
    DETACH DELETE n
    RETURN count(n) AS deletedCount
    """
    result = tx.run(query, batchSize=batch_size)
    record = result.single()
    if record:
        return record["deletedCount"]
    return 0

def delete_all_nodes_batched(driver, batch_size=5000):
    total_deleted = 0
    start_time = time.time()

    with driver.session() as session:
        while True:
            deleted_count = session.execute_write(delete_graph_batch, batch_size)
            total_deleted += deleted_count

            if deleted_count > 0:
                elapsed_time = time.time() - start_time
                print(f"Deleted {deleted_count} nodes and relationships in this batch. Total deleted: {total_deleted} in {elapsed_time:.2f} seconds.")

            if deleted_count == 0:
                break

    elapsed_total_time = time.time() - start_time
    print(f"Total of {total_deleted} nodes and relationships deleted in {elapsed_total_time:.2f} seconds.")

    driver.close()
    print("Driver closed.")

if __name__ == "__main__":
    if driver:
        drop_all_indexes(driver)
        delete_all_nodes_batched(driver)
    else:
        logging.error("Driver failed to initialize.")