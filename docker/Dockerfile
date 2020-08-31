FROM grizly_base:latest


# create and give priviledges to users
RUN useradd --create-home analyst && \
    usermod -aG sudo analyst && \
    find /usr/local/lib -type d -exec chmod 777 {} \; && \
    find /usr/local/bin -type d -exec chmod 777 {} \;

WORKDIR /grizly

COPY . .
RUN export https_proxy=$HTTPS_PROXY && \
    pip install --upgrade pip && \
    python setup.py clean --all && \
    python setup.py install && \
    pip install -r requirements.txt && \
    pip install -r requirements-dev.txt && \
    rm -rf /grizly

ARG USER=analyst
ENV USER ${USER}
USER $USER
ENV HOME="/home/$USER"
WORKDIR ${HOME}


ENV DASK_SCHEDULER_ADDRESS="tcp://dask_scheduler:8786"
ENV GRIZLY_WORKFLOWS_HOME="${HOME}/workflows"
ENV GRIZLY_DASK_SCHEDULER_ADDRESS=localhost:8786
ENV GRIZLY_REDIS_HOST=localhost
ENV GRIZLY_REDIS_PORT=6379
ENV GRIZLY_CONFIG_FILE="${HOME}/.grizly/config.json"

ENV JUPYTER_ALLOW_INSECURE_WRITES=1

ARG BUILD_DATE

LABEL maintainer="acoe_team@te.com"
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.name="acoeteam/grizly_notebook"
LABEL org.label-schema.description="A Jupyter Notebook environment for running the grizly framework"
LABEL org.label-schema.url="https://grizly-docs.readthedocs.io/"
LABEL org.label-schema.build-date=$BUILD_DATE
