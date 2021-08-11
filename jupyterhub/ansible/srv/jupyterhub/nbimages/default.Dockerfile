FROM docker.chameleoncloud.org/jupyterhub-user:latest 

USER root

RUN apt-get update -y && apt-get install -y rsync \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

ADD nbroot/* /etc/jupyter/serverroot/
ADD nb_start.sh /usr/local/bin/before-notebook.d/00-on-start.sh

#
# Put additional customizations below here!
#

USER $NB_USER