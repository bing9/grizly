FROM python:3.8-buster AS odbc_drivers

ARG HTTPS_PROXY
ARG HTTP_PROXY
ARG docker_user
ENV DOCKER_USER=$docker_user


# Make apt and wget work behind proxy
RUN touch /etc/apt/apt.conf.d/proxy.conf && \
    echo 'Acquire::http::Proxy' '"'${HTTP_PROXY}'"'';' > /etc/apt/apt.conf.d/proxy.conf

RUN touch /etc/wgetrc && \
    echo "http_proxy = $HTTP_PROXY" > /etc/wgetrc && \
    echo "https_proxy = $HTTPS_PROXY" >> /etc/wgetrc


## System packages
RUN apt-get update &&\
    yes | apt-get install git python3-dev unixodbc-dev g++ libboost-all-dev\
    apt-utils vim unixodbc odbc-postgresql awscli wkhtmltopdf \
    fonts-font-awesome nodejs graphviz sudo ca-certificates npm && \
    rm -rf /var/lib/apt/lists/*

## External packages
WORKDIR /opt

#Creating PDFs from HTML
RUN wget https://github.com/wkhtmltopdf/wkhtmltopdf/releases/download/0.12.3/wkhtmltox-0.12.3_linux-generic-amd64.tar.xz --no-check-certificate && \
    tar -vxf wkhtmltox-0.12.3_linux-generic-amd64.tar.xz && \
    rm -rf wkhtmltox-0.12.3_linux-generic-amd64.tar.xz

## Databases, ODBC
# Redshift
RUN wget https://redshift-downloads.s3.amazonaws.com/drivers/odbc/1.4.10.1000/AmazonRedshiftODBC-64-bit-1.4.10.1000-1.x86_64.deb && \
    apt install ./AmazonRedshiftODBC-64-bit-1.4.10.1000-1.x86_64.deb && \
    rm -rf AmazonRedshiftODBC-64-bit-1.4.10.1000-1.x86_64.deb

# Denodo
ENV DENODO_HOME="/opt/denodo"
COPY src/Denodo Denodo/
RUN tar -vxf Denodo/denodo-vdp-odbcdriver.tar.gz && \
    rm -rf Denodo



FROM odbc_drivers AS python_env

RUN export https_proxy=$HTTPS_PROXY && \
    pip3 install --upgrade pip && \
    pip3 install -r requirements.txt && \
    pip3 install -r requirements-dev.txt && \
    rm -f requirements.txt requirements-dev.txt



FROM python_env AS jupyter_extensions

ARG ALTERNATIVE_PROXY
ENV JUPYTER_ALLOW_INSECURE_WRITES=1

#Install jupyterlab extensions
#Set NPM proxy
RUN npm config set proxy $HTTPS_PROXY &&\
    npm config set https-proxy $HTTPS_PROXY &&\
    npm config set strict-ssl=false

# Set YARN proxy inside jupyterlab
ENV PYTHON_VERSION=$(python -V 2>&1 | awk {'print $2'})
ENV YARN_PATH="/usr/local/lib/$PYTHON_VERSION/site-packages/jupyterlab/staging/yarn.js"
RUN node $YARN_PATH install --non-interactive && \
    node $YARN_PATH config set proxy $ALTERNATIVE_PROXY && \
    node $YARN_PATH config set https-proxy $ALTERNATIVE_PROXY && \
    node $YARN_PATH config set "strict-ssl" false -g

# #Install jupyterlab extensions
ENV NODE_OPTIONS="--max-old-space-size=4096"
RUN jupyter labextension install @jupyterlab/toc --no-build && \
    jupyter labextension install dask-labextension --no-build &&\
    jupyter lab build && \
    jupyter serverextension enable dask_labextension && \
    jupyter serverextension enable --py jupyterlab_git && \
    jupyter lab clean && \
    jlpm cache clean && \
    npm cache clean --force && \
    rm -rf $HOME/.node-gyp && \
    rm -rf $HOME/.local && \
    jupyter serverextension enable dask_labextension



FROM jupyter_extensions AS grizly_notebook

ENV DASK_SCHEDULER_ADDRESS="tcp://dask_scheduler:8786"
ENV HOME="/home/analyst"

# create and give priviledges to the analyst user
RUN useradd --create-home analyst && \
    usermod -aG sudo analyst && \
    find /usr/local/lib -type d -exec chmod 777 {} \; && \
    find /usr/local/bin -type d -exec chmod 777 {} \;
USER analyst


WORKDIR ${HOME}

COPY .config/* .
# appdend odbcini configs
# COPY .config/odbcinst.ini .
# RUN echo "" >> /etc/odbcinst.ini && \
#     cat odbcinst.ini >> /etc/odbcinst.ini && \
#     rm -f odbcinst.ini


COPY .config/odbc.ini /etc
# COPY .config/pip .config/pip


# grizly
ARG GITHUB_TOKEN
RUN git clone $GITHUB_TOKEN/tedcs/grizly.git -b 0.3.7 && \
    cd grizly && \
    python setup.py install && \
    pip install -r requirements.txt && \
    cd ../ && \
    rm -rf grizly


ENV GRIZLY_WORKFLOWS_HOME="${HOME}/workflows"
ENV GRIZLY_DASK_SCHEDULER_ADDRESS=dask_scheduler:8786
ENV GRIZLY_REDIS_HOST=localhost
ENV GRIZLY_REDIS_PORT=6379
ENV GRIZLY_CONFIG_FILE="${HOME}/.grizly/config.json"

ARG BUILD_DATE

LABEL maintainer="acoe_team@te.com"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.name="acoeteam/grizly_notebook"
LABEL org.label-schema.description="A Jupyter Notebook environment for running the grizly framework"
LABEL org.label-schema.url="https://grizly-docs.readthedocs.io/"
LABEL org.label-schema.build-date=$BUILD_DATE

ENTRYPOINT ["/bin/bash", "-c", "jupyter notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root --NotebookApp.token=''"]
