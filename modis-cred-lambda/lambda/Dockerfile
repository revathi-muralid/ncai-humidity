FROM public.ecr.aws/lambda/python:3.9

# Install any other dependencies here...

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .  
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}" --use-feature=2020-resolver

# Copy function code
COPY download_hadisd_lambda.py ${LAMBDA_TASK_ROOT}

COPY hadisd_station_info_v330_2022f.txt ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "download_hadisd_lambda.lambda_handler" ] 
