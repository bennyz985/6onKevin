# Half a dozen degrees of Kevin Bacon

This project builds a Neo4j graph database using a subset of the IMDB movie database and then leverages Neo4j's GDS and Apoc libraries to find relationship chains and node similarity to Kevin Bacon.

## Neo4J Installation

This code builds out a graph database using Neo4j.  The installation of Neo4j desktop is a prerequisite. [Neo4J Desktop Download](https://neo4j.com/product/developer-tools/)

after installation add the 'GDS' and 'APOC' plug-ins. [Enabling plugins video](https://www.youtube.com/watch?v=b1Yr2nHNS4M)


## Config, settings and general setup
Default database config may need to be edited to allow full permissions to the 'APOC' and 'Graph Data Science' libraries.
```bash
dbms.security.procedures.unrestricted=apoc.*,gds.*,dbms.*
dbms.security.procedures.allowlist=apoc.*,gds.*,dbms.*
```

Git LFS needs to be installed and initialized.  This is needed for storage of the IMDB files that are used to construct the graph. [Git LFS website](https://git-lfs.com/).

Environment variables.  Neo4j connection details need to be established in a `.env` file

```bash
        NEO4J_URI="bolt://localhost:7687"
        NEO4J_USERNAME="neo4j"
        NEO4J_PASSWORD="YourSecureNeo4jPassword"
        DATA_DIRECTORY="YourLocalDirectoryFilePath"
```

## Source data
The necessary IMDB dataset files (`title.tsv`, `name.tsv`, `principals.tsv`) are located in the `data_files` (should get coppied automatically when you clone the repository).


## License
This project makes use of a subset of IMDB's non-commercial dataset
[IMDB dataset](https://developer.imdb.com/non-commercial-datasets/)



