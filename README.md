# Half a dozen degrees of Kevin Bacon

This project builds a Neo4j graph database using a subset of the IMDB movie database and then leverages GDS and Apoc to find relationships and similarity to Kevin Bacon.

## Installation

This code builds out a graph database using Neo4j.  The installation of Neo4j desktop is a prerequisite. [Neo4J Desktop Download](https://neo4j.com/product/developer-tools/)

after installation add the 'GDS' and 'APOC' plug-ins. [Enabling plugins video](https://www.youtube.com/watch?v=b1Yr2nHNS4M)

**Prerequisites:**

1.  **Install Git LFS:** If you don't have it already, install Git LFS on your system. You can find instructions on the [Git LFS website](https://git-lfs.com/).

2.  **Initialize Git LFS:** After cloning the repository, navigate to the project directory in your terminal and run:
    ```bash
    git lfs install
    ```

3.  **IMDB Dataset Files:** The necessary IMDB dataset files (`title.tsv`, `name.tsv`, `principals.tsv`) are located in the `data_files` (should get coppied automatically when you clone the repository).

4.  **Environment Variables (for Neo4j):**

    * Create a `.env` file in the root of the project.
    * Add your Neo4j connection details:

        ```
        NEO4J_URI="bolt://localhost:7687"
        NEO4J_USERNAME="neo4j"
        NEO4J_PASSWORD="YourSecureNeo4jPassword"
        ```


## Database config settings
Default database config may also need to be edited to allow full permissions to the 'APOC' and 'Graph Data Science' libraries.
```bash
dbms.security.procedures.unrestricted=apoc.*,gds.*,dbms.*
dbms.security.procedures.allowlist=apoc.*,gds.*,dbms.*
```


## License
This project makes use of a subset of IMDB's non-commercial dataset
[IMDB dataset](https://developer.imdb.com/non-commercial-datasets/)