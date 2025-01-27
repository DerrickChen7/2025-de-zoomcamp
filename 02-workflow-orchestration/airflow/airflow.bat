::Build the image. It may take several minutes You only need to do this the first time you run Airflow or if you modified the Dockerfile or the requirements.txt file.
docker-compose build

::Initialize configs:
docker-compose up airflow-init

::Run Airflow
docker-compose up -d