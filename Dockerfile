FROM jupyter/all-spark-notebook
#FROM mailshanx/nyc_data:v1

LABEL maintainer="Satish Shankar"

# Open Ports for Jupyter
EXPOSE 7745

USER root
#USER $NB_UID

# Install pyspark
#RUN conda install --quiet --yes pyspark && \
#    conda clean -tipsy && \
#    fix-permissions $CONDA_DIR && \
#    fix-permissions /home/$NB_USER

#Install pyarrow
#RUN conda install --quiet --yes pyarrow && \
#    conda clean -tipsy && \
#    fix-permissions $CONDA_DIR && \
#    fix-permissions /home/$NB_USER

#setup filesystem
RUN mkdir /home/jovyan/work/nyc_data
RUN fix-permissions /home/jovyan/work/nyc_data


ENV SHELL=/bin/bash

VOLUME /home/jovyan/work/nyc_data
WORKDIR /home/jovyan/work/nyc_data


ADD start.sh /home/jovyan/work/nyc_data/start.sh
ADD run_jupyter.sh /home/jovyan/work/nyc_data/run_jupyter.sh


CMD ["./start.sh"]

#CMD  ["./run_jupyter.sh"]
#CMD ["/bin/bash"]
#refer to https://github.com/nilesh-patil/datascience-environment/blob/master/Dockerfile, https://github.com/jupyter/docker-stacks/blob/master/base-notebook/Dockerfile, and https://github.com/jupyter/docker-stacks/blob/master/all-spark-notebook/Dockerfile