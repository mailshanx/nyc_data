#! /bin/bash
#use this to start jupyter notebook at port 7745 from container
jupyter notebook --no-browser --NotebookApp.token='' --ip=0.0.0.0 --allow-root --port=7745 