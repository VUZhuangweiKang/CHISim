# Copyright (c) The University of Chicago
# Distributed under the terms of the Modified BSD License.

# Configuration file for JupyterHub
import os
import sys
import hashlib

from urllib.parse import parse_qsl, unquote, urlencode
from dockerspawner import DockerSpawner
from jupyterhub.handlers import BaseHandler
from jupyterhub.utils import url_path_join
from tornado import web
from tornado.httputil import url_concat

c = get_config()

# Must match name of container
c.JupyterHub.hub_ip = 'hub'

c.JupyterHub.db_url = '/srv/jupyterhub/db/jupyterhub.sqlite'
c.JupyterHub.cookie_secret_file = '/srv/jupyterhub/crypt_key'

##################
# Logging
##################

c.Application.log_level = 'DEBUG'
c.JupyterHub.log_level = 'DEBUG'
c.Spawner.debug = True
c.DockerSpawner.debug = True
# To allow debugging containers that failed to start, set this to False
c.DockerSpawner.remove_containers = False

##################
# Base spawner
##################

# This is where we can do other specific bootstrapping for the user environment
def pre_spawn_hook(spawner):
    shasum = hashlib.sha256()
    shasum.update(spawner.user.name.encode("utf-8"))
    uid_offset = 3000  # UID start range
    uid = int.from_bytes(shasum.digest(), "big", signed=False) % 100
    
    spawner.environment['NB_USER'] = spawner.user.name
    spawner.environment['NB_UID'] = uid_offset + uid

origin = '*'
c.Spawner.args = ['--NotebookApp.allow_origin={0}'.format(origin)]
c.Spawner.pre_spawn_hook = pre_spawn_hook
c.Spawner.mem_limit = '2G'
c.Spawner.http_timeout = 600
c.Spawner.default_url = '/lab'


##################
# Docker spawner
##################

# Spawn single-user servers as Docker containers wrapped by the option form
c.JupyterHub.spawner_class = DockerSpawner

c.DockerSpawner.name_template = '{prefix}-{username}'

default_image = 'notebook-default:latest'
c.DockerSpawner.allowed_images = {
    'Default': default_image,
    # Add more images here; a form will automatically be displayed for users
    # to pick between them when there is more than 1 option.
}
# When allowed_images contains only 1 value, this additional config option is used.
c.DockerSpawner.image = default_image

# Connect containers to this Docker network
network_name = os.environ['DOCKER_NETWORK_NAME']
c.DockerSpawner.use_internal_ip = True
c.DockerSpawner.network_name = network_name
# Pass the network name as argument to spawned containers
c.DockerSpawner.extra_host_config = { 'network_mode': network_name }
c.DockerSpawner.extra_create_kwargs.update({
    'user': 'root',
})

# This directory will be symlinked to the `notebook_dir` at runtime.
c.DockerSpawner.cmd = ['start-notebook.sh']
# Mount the real user's Docker volume on the host to the
# notebook directory in the container for that server
c.DockerSpawner.volumes = {
    '{prefix}-{username}': '/home/{username}',
    'shared-data': {
        'bind': '/data',
        'mode': 'ro',
    },
}
c.DockerSpawner.environment = {
    # Enable JupyterLab application
    'JUPYTER_ENABLE_LAB': 'yes',
    'CHOWN_HOME': 'yes',
    'CHOWN_HOME_OPTS': '-R',
}

if os.environ.get('GRANT_SUDO', '').lower() == 'yes':
    c.DockerSpawner.environment.update({
        # Allow users to have sudo access within their container
        'GRANT_SUDO': 'yes',
    })

##################
# Authentication
##################

c.Authenticator.admin_users = {'admin',}
c.JupyterHub.authenticator_class = 'nativeauthenticator.NativeAuthenticator'
