from neo4j import GraphDatabase
import time

uri = "bolt://localhost:7687"
username = "neo4j"
password = "SixKevin"
driver = GraphDatabase.driver(uri, auth=(username, password))

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

