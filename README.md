# Query Execution component
This component is part of the Data Lake. The query component has the purpose of perform, manage and orchestrate the steps needed to access the data contained in the data lake as if it were contained in a single database.


## Build from sources
### JVM
Build using Maven:

```
mvn clean compile assembly:single
```


Run query execution component:

`java -jar target\QueryExecution-0.0.1-SNAPSHOT-jar-with-dependencies.jar {service registry prototype URL} {TCP port}`


Default TCP port for the REST API: 4570

Swagger description of the API:   http://localhost:4570/swagger



### Docker
Build Docker image:

`docker build --no-cache -t docker-activage.satrd.es/dl-query-component:<version> .`


Run in Docker:

`docker run -d -p {TCP port}:4570 --name query-component docker-activage.satrd.es/dl-query-component:<version> {service registry prototype URL}`


## Dependencies
The Data Lake Query component depends on the following components:


### Web service registry prototype

The current version uses JSON server as web service registry prototype:  

https://github.com/typicode/json-server



Deploy JSON server on Docker: 

`docker run -d -p {local TCP port}:80 -v {path to JSON file}:/data/db.json --name json-server clue/json-server`


Example JSON database file: db.json


Add **historical data web services** (including Independent Data Storage) under **"services"**. The id of the Independent Data Storage must be the name of the DB where the the historical data are stored and the type must be "independent-storage". For other historical data web services, use type="platform-historic". The attributes "DS" and "platform" correspond to the DS name and type of the  platform associated with this web service (for example, universAAL), respectively, and can be used as input parameters in the queries. The attribute "sources" contains the identifiers of the IoT platforms associated with a service. In the case of historical data web services, platform data is used to determine the syntactic and semantic translation of the retrieved data.

Add **translation web services** under **"services"**. For the IPSM, use type="semantic-translator". Use type="syntactic-translator" for the syntactic translation web services. The attribute "platformType" identifies the platform type associated with a translator and should have the same value as in the SIL (for example, "http://inter-iot.eu/UniversAAL").

Add platform data under **"platforms"**. The attribute "platformType" defines the syntactic translation of the data. The Alignment attributes define the semantic translation of the data (only upstream alignments are used in historical data translation).


### Syntactic translation web services

You can deploy an available syntactic translation web service, which supports universAAL, Fiware and SOFIA2 syntax (associated platform type values: "http://inter-iot.eu/UniversAAL", "http://inter-iot.eu/FIWARE" and "http://inter-iot.eu/sofia2", respectively).


Deploy the syntactic translator on Docker:

`docker run -d -p 4568:4568 --name syntactic-translator docker-activage.satrd.es/syntactic-translator:0.0.1`


### Semantic translation

Deploy the IPSM to allow the semantic translation of the data (the IPSM is part of the SIL deployment).
