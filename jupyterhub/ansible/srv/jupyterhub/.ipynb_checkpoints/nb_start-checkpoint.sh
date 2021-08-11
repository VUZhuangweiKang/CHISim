#!/usr/bin/env bash

chown -R ${NB_UID}:${NB_GID} /etc/jupyter/serverroot
rsync -aq /etc/jupyter/serverroot/ /home/${NB_USER}/