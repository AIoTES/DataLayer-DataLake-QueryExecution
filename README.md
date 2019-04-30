# QueryExecution
### Run query execution component


```
mvn clean compile assembly:single
```


`java -jar target\QueryExecution-0.0.1-SNAPSHOT-jar-with-dependencies.jar {service registry prototype URL} {TCP port}`



### Web service registry prototype

The current version uses JSON server as web service registry prototype:  

https://github.com/typicode/json-server



Deploy JSON server on Docker: 

`docker run -d -p {local TCP port}:80 -v {path to JSON file}:/data/db.json --name json-server clue/json-server`


Example JSON database file: db.json


Add **historic data webservices** (including Independent Data Storage) under **"services"**. The id of the Independent Data Storage must be the name of the DB where the the historic data are stored and the type must be "independent-storage". For other historic data web services, use type="platform-historic". The attribute "platforms" contains the identifiers of the IoT platfoms associated with a service. In the case of historic data webservices, platform data is used to determine the syntactic and semantic translation of the retrieved data.

Add **translation web services** under **"services"**. For the IPSM, use type="semantic-translator".

Add platform data under **"platforms"**. The attribute "platformType" defines the syntactic translation of the data (supported values: "http://inter-iot.eu/UniversAAL", "http://inter-iot.eu/FIWARE" and "http://inter-iot.eu/sofia2"). The Alignment attributes define the semantic translation of the data.


