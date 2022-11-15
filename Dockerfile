FROM jupyter/datascience-notebook:latest

# Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Install Polars
RUN pip install polars xarray awswrangler boto3 pyathena pandas matplotlib mpl_toolkits pyarrow uuid


