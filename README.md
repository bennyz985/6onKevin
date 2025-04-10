# Half a dozen degrees of Kevin Bacon

This project builds a Neo4j graph database using a subset of the IMDB movie database and then leverages GDS and Apoc to find relationships and similarity to Kevin Bacon.

## Installation

This code builds out a graph database using Neo4j.  The installation of Neo4j desktop is a prerequisite. [Neo4J Desktop Download](https://neo4j.com/product/developer-tools/)

after installation add the 'GDS' and 'APOC' plug-ins. [Enabling plugins video](https://www.youtube.com/watch?v=b1Yr2nHNS4M)

## Database config settings
Default database config may also need to be edited to allow full permissions to the 'APOC' and 'Graph Data Science' libraries.
```bash
dbms.security.procedures.unrestricted=apoc.*,gds.*,dbms.*
dbms.security.procedures.allowlist=apoc.*,gds.*,dbms.*
```


## License
This project makes use of a subset of IMDB's non-commercial dataset
[IMDB dataset](https://developer.imdb.com/non-commercial-datasets/)