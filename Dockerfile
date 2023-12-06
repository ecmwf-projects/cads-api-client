FROM continuumio/miniconda3

WORKDIR /src/cads-api-client

COPY environment.yml /src/cads-api-client/

RUN conda install -c conda-forge gcc python=3.11 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-api-client

RUN pip install --no-deps -e .
