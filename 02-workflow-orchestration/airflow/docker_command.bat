::Build the image. It may take several minutes You only need to do this the first time you run Airflow or if you modified the Dockerfile or the requirements.txt file.
docker compose build

::Initialize configs:
docker compose up airflow-init

::Run Airflow
docker compose up -d

::Open a shell inside the container
docker exec -it <container_id_or_name> bash
