# first-saga-pattern-app
Implementation of the Saga standard to deal with transactions distributed over microservices


## Orchestrated saga pattern


Saga pattern: https://microservices.io/patterns/data/saga.htmlhttps://microservices.io/patterns/data/saga.html
## Tecnologias

* **Java 17**
* **Spring Boot 3**
* **Apache Kafka**
* **API REST**
* **PostgreSQL**
* **MongoDB**
* **Docker**
* **docker-compose**
* **Redpanda Console**

## Architecture

* Order-Service: microservice responsible only for generating an initial order and receiving a notification. This is where we will have REST endpoints to start the process and retrieve the event data. The database used will be MongoDB.
* Orchestrator-Service**: microservice responsible for orchestrating the entire Saga execution flow, it will know which microservice has been executed and in which state, and to which will be the next microservice to be sent, this microservice will also save the events process. This service does not have a database.
**Product-Validation-Service**: microservice responsible for validating whether the product informed in the order exists and is valid. This microservice will store the validation of a product for an order ID. The database used will be PostgreSQL.
* Payment Service: microservice responsible for making a payment based on the unit values and quantities entered in the order. This microservice will store the payment information for an order. The database used will be PostgreSQL.
* Inventory-Service: microservice responsible for writing off the stock of the products in an order. This microservice will store the information from the write-off of a product for an order ID. The database used will be PostgreSQL.


## Kafka Topics
![xd](https://github.com/user-attachments/assets/3fd33986-ed9c-4b75-a28d-bd0b4f7bd2ce)

![image](https://github.com/user-attachments/assets/6eecb849-ecd7-4b27-923c-6d06bef0caae)



