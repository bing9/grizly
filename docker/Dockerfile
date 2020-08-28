FROM python:3.8.5-buster AS odbc_drivers

ARG HTTP_PROXY
ARG HTTPS_PROXY

# Make apt and wget work behind proxy
RUN touch /etc/apt/apt.conf.d/proxy.conf && \
    echo 'Acquire::http::Proxy' '"'${HTTP_PROXY}'"'';' > /etc/apt/apt.conf.d/proxy.conf

RUN touch /etc/wgetrc && \
    echo "http_proxy = $HTTP_PROXY" > /etc/wgetrc && \
    echo "https_proxy = $HTTPS_PROXY" >> /etc/wgetrc

RUN apt update && \
    apt install -y unixodbc-dev unixodbc odbc-postgresql wkhtmltopdf

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
COPY docker/src/Denodo Denodo/
RUN tar -vxf Denodo/denodo-vdp-odbcdriver.tar.gz && \
    rm -rf Denodo

# Appdend odbcini configs
COPY docker/.config/odbcinst.ini .
RUN echo "" >> /etc/odbcinst.ini && \
    cat odbcinst.ini >> /etc/odbcinst.ini && \
    rm -f odbcinst.ini



FROM odbc_drivers AS dependencies

COPY grizly .
COPY setup.py . 
COPY README.md .
COPY requirements.txt .
COPY requirements-dev.txt .


RUN apt update && \
    apt install -y python3-dev build-essential libboost-all-dev g++ && \
    export https_proxy=$HTTPS_PROXY && \
    pip install --upgrade pip && \
    pip install . --user && \
    pip install --user -r requirements.txt && \
    pip install --user -r requirements-dev.txt



FROM dependencies AS jupyter_setup

ENV JUPYTER_ALLOW_INSECURE_WRITES=1

# Install nodejs
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash - && \
    apt install -y nodejs


# Install jupyterlab extensions
# Set npm proxy

RUN npm config set proxy $HTTPS_PROXY &&\
    npm config set https-proxy $HTTPS_PROXY &&\
    npm config set strict-ssl=false

# Set Yarn proxy
ARG ALTERNATIVE_PROXY
ENV YARN_PATH="/root/.local/lib/python3.8/site-packages/jupyterlab/staging/yarn.js"
RUN node $YARN_PATH install -q --non-interactive && \
    node $YARN_PATH config -s set proxy $ALTERNATIVE_PROXY && \
    node $YARN_PATH config -s set https-proxy $ALTERNATIVE_PROXY && \
    node $YARN_PATH config set "strict-ssl" false -g

# JupyterLab extensions
ENV NODE_OPTIONS="--max-old-space-size=4096"
ENV PATH=/root/.local/bin:$PATH
# WORKDIR /root/.local/bin
RUN jupyter labextension install @jupyterlab/toc --no-build && \
    jupyter labextension install dask-labextension --no-build &&\
    jupyter lab build --dev-build=False && \
    jupyter serverextension enable dask_labextension && \
    jupyter serverextension enable --py jupyterlab_git && \
    jupyter lab clean && \
    jlpm cache clean && \
    npm cache clean --force && \
    rm -rf $HOME/.node-gyp && \
    rm -rf $HOME/.local && \
    jupyter serverextension enable dask_labextension



FROM python:3.8.5-buster

ENV DASK_SCHEDULER_ADDRESS="tcp://dask_scheduler:8786"

## System packages
RUN apt update && \
    yes | apt install git python3-dev unixodbc-dev g++ libboost-all-dev \
    apt-utils vim unixodbc odbc-postgresql awscli wkhtmltopdf build-essential \
    fonts-font-awesome graphviz sudo ca-certificates && \
    rm -rf /var/lib/apt/lists/*


# create and give priviledges to the analyst user
RUN useradd --create-home analyst && \
    usermod -aG sudo analyst && \
    find /usr/local/lib -type d -exec chmod 777 {} \; && \
    find /usr/local/bin -type d -exec chmod 777 {} \;
USER analyst
ENV HOME="/home/analyst"

WORKDIR ${HOME}


COPY --from=odbc_drivers /opt /opt
COPY --from=odbc_drivers /etc/odbcinst.ini /etc/odbcinst.ini
COPY --from=jupyter_setup /root/.local $HOME/.local
ENV PATH=$HOME/.local/bin:$PATH


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
