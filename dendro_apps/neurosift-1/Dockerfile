FROM continuumio/miniconda3

# Install opencv from conda-forge (pypi version is not sufficient)
RUN conda install -y -c conda-forge opencv

# Install ffmpeg
RUN apt-get update && apt-get install -y libgl1-mesa-glx libgl1-mesa

# Install dendro from source
RUN git clone https://github.com/flatironinstitute/dendro.git && \
    cd dendro && \
    git checkout main && \
    cd python && \
    pip install -e .
# RUN pip install dendro==0.2.14

# Install kachery-cloud
RUN pip install kachery-cloud

# Install lindi
RUN git clone https://github.com/neurodatawithoutborders/lindi.git && \
    cd lindi && \
    git checkout dev && echo "aa" && \
    pip install -e .

# Install neurosift (for codecs)
RUN pip install neurosift

# Copy files into the container
RUN mkdir /app
COPY *.py /app/
COPY autocorrelograms/*.py /app/autocorrelograms/
COPY autocorrelograms/helpers/*.py /app/autocorrelograms/helpers/
COPY compressed_videos/*.py /app/compressed_videos/
