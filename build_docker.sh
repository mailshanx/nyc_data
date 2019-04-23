#! /bin/bash
echo "building docker image"
docker build -t mailshanx/nyc_data -f ./Dockerfile .
echo "executing docker run, exposing 7745 for Jupyter Notebook and 4040 for Spark UI"
docker run -it -p 7745:7745 -p 4040:4040 -v $(pwd):/home/jovyan/work/nyc_data mailshanx/nyc_data
#echo "starting shell session into container"
#docker exec -it $(docker container ls | grep "nyc_data" | awk '{print $1}') bash