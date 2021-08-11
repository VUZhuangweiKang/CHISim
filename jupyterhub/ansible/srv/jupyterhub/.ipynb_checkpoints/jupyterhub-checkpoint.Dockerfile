FROM jupyterhub/jupyterhub:1.2

RUN pip3 install dockerspawner

ADD nativeauthenticator ./nativeauthenticator
RUN pip3 install ./nativeauthenticator
