##Run as a simple script:

The main.py file serves as a simple script that can be used in terminal like:

python main.py inputfilename outputfilename

the inputfilename and outputfilename are meant to be switched to real values. A file link from a public AWS bucket is fine. E.g.: http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv

and output can be any directory valid in your local.

##Run as an Airflow Job:

Before running the Airflow job, please enter valid output file location as a class constant at the beginning.

To initialize airflow in the docker container, run:

`docker-compose up airflow-init`

This creates an admin user with login `airflow` and password `airflow`

Then to start all the services, run:

`docker-compose up`

It is ready when you see logs like this:

`127.0.0.1 - - [26/Aug/2021:02:04:26 +0000] "GET /health HTTP/1.1" 200 187 "-" "curl/7.64.0"`

You can view, trigger, monitor the job on UI generated, by default it's at `localhost:8080`. Login details are as previously generated.

To clean up, run:

`docker-compose down --volumes --rmi all`