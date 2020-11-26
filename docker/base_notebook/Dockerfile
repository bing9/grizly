FROM python:3.8.6-buster

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG ALTERNATIVE_PROXY
ENV JUPYTER_ALLOW_INSECURE_WRITES=1

# make apt and wget work behind proxy
RUN touch /etc/apt/apt.conf.d/proxy.conf && \
    echo 'Acquire::http::Proxy' '"'${HTTP_PROXY}'"'';' > /etc/apt/apt.conf.d/proxy.conf

RUN touch /etc/wgetrc && \
    echo "http_proxy = $HTTP_PROXY" > /etc/wgetrc && \
    echo "https_proxy = $HTTPS_PROXY" >> /etc/wgetrc


## System packages
RUN apt-get update &&\
    yes | apt-get install git python3-dev unixodbc-dev g++ libboost-all-dev\
    apt-utils vim unixodbc odbc-postgresql awscli wkhtmltopdf \
    fonts-font-awesome graphviz sudo ca-certificates zsh pandoc && \
    rm -rf /var/lib/apt/lists/*

## Beautify shell
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" && \
    git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k && \
    echo 'ZSH_THEME="powerlevel10k/powerlevel10k"' >> ~/.zshrc
# TODO: add premade .zshrc and .p10k.zsh

# Java stuff is "special"
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash - && \
    apt install -y nodejs

## External packages
WORKDIR /opt
#Creating PDFs from HTML
RUN wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.buster_amd64.deb --no-check-certificate && \
    apt install -y ./wkhtmltox_0.12.6-1.buster_amd64.deb && \
    rm -rf wkhtmltox_0.12.6-1.buster_amd64.deb

## Databases, ODBC
# Redshift
RUN wget https://s3.amazonaws.com/redshift-downloads/drivers/odbc/1.4.17.1000/AmazonRedshiftODBC-64-bit-1.4.17.1000-1.x86_64.deb && \
    apt install ./AmazonRedshiftODBC-64-bit-1.4.17.1000-1.x86_64.deb && \
    rm -rf AmazonRedshiftODBC-64-bit-1.4.17.1000-1.x86_64.deb

# Denodo
ENV DENODO_HOME="/opt/denodo"
COPY docker/base_notebook/Denodo Denodo/
RUN tar -vxf Denodo/denodo-vdp-odbcdriver.tar.gz && \
    rm -rf Denodo

# appdend odbcini configs
COPY docker/docker_home/.config/odbcinst.ini .
RUN echo "" >> /etc/odbcinst.ini && \
    cat odbcinst.ini >> /etc/odbcinst.ini && \
    rm -f odbcinst.ini

COPY requirements.txt .
COPY requirements-dev.txt .
RUN export https_proxy=$HTTPS_PROXY && \
    pip3 install --upgrade pip && \
    pip3 install -r requirements.txt && \
    pip3 install -r requirements-dev.txt && \
    rm -f requirements.txt requirements-dev.txt


WORKDIR /code/start

#Install jupyterlab extensions
#Set NPM proxy
RUN npm config set proxy $HTTPS_PROXY &&\
    npm config set https-proxy $HTTPS_PROXY &&\
    npm config set strict-ssl=false

# Set YARN proxy inside jupyterlab
# Note to adjust the path when new version of python is installed
ENV YARN_PATH=/usr/local/lib/python3.8/site-packages/jupyterlab/staging/yarn.js
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
