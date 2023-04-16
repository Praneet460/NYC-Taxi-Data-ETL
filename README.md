# NYC-Taxi-Data-ETL
The ETL Pipeline Projects on NYC Taxi Dataset

### How To Run
- To run Postgres database and PG Admin in the same network execute `docker-compose up`
-  Connect PG Admin server with Postgres DB by visiting `localhost:8080/`

### How to Ingest Data to Postgres DB
- To ingest data in Postgres DB use following steps
- Step1 : Build an image `docker build -t nyc_taxi_ingest:v001 .`
- Step2 : Run created image in a container 
```
docker network ls

# assign --network with above service common bridge network

docker run -it \
    --network=nyc-taxi-data-etl_default \
    nyc_taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz" \
    --csv_name="yellow_tripdata_2021-01" \
    --iterations=3
```


### Available Ports
- PG Admin : `8080`
- Postgres DB: `5432`
