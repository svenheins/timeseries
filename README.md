# timeseries

evaluation of timeseries database

## installation

python=3.10

conda create -n finance python=3.10

## influxDB

setting up the db is straight forward:

1. login to http://localhost:8086/
2. setup the user, password, bucket, company name
3. retrieve the token and save it to the .env-file to the TOKEN variable
4. now you can run the code