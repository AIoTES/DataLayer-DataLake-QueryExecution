# QueryExecution

mvn clean compile assembly:single


java -jar target\QueryExecution-0.0.1-SNAPSHOT-jar-with-dependencies.jar {service registry prototype URL} {TCP port} 


The current version uses JSON server as service registry prototype:  

https://github.com/typicode/json-server


Deploy JSON server on Docker: 

docker run -d -p {local TCP port}:80 -v {path to JSON database}:/data/db.json --name json-server clue/json-server


Example JSON database: db.json


Add historic data webservices (including Independent Data Storage) under "historic-webservices". The id of the Independent Data Storage must be the name of the DB where the the historic data are stored and the type must be "common". For other historic data web services, use type="platform". The attribute "platformType" defines the syntactic translation of the data (supported values: "http://inter-iot.eu/UniversAAL", "http://inter-iot.eu/FIWARE" and "http://inter-iot.eu/sofia2"). The Alignment attributes define the semantic translation of the data.


Add translation services under "services". For the IPSM, use type="semantic-translator".
