FROM python:3.9

# Install dendro
RUN pip install dendro==0.2.12

# Install lindi
RUN git clone https://github.com/neurodatawithoutborders/lindi.git && \
    cd lindi && \
    git checkout write && \
    pip install -e .

# Install lindi-cloud
RUN git clone https://github.com/magland/lindi-cloud.git && \
    cd lindi-cloud && \
    pip install -e .

# Copy files into the container
RUN mkdir /app
COPY *.py /app/
COPY autocorrelograms/*.py /app/autocorrelograms/
COPY autocorrelograms/helpers/*.py /app/autocorrelograms/helpers/
