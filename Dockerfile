FROM jupyter/datascience-notebook:latest

# Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Install Polars
RUN pip install polars xarray awswrangler boto3 s3 s3fs

RUN pip install pyathena pandas pyarrow uuid us numpy missingno

RUN conda install -c conda-forge python-eccodes pandas

RUN pip install pdbufr


