FROM jupyter/datascience-notebook:latest

# Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Install Polars
RUN pip install polars xarray awswrangler boto3 s3 s3fs qc_utils

RUN pip install pyathena metpy pandas pyarrow uuid us numpy missingno folium

RUN pip install pdbufr plotly zarr netCDF4


