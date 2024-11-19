# # Use AWS Lambda's base Python 3.11 image
# FROM public.ecr.aws/lambda/python:3.11

# # Set the working directory inside the container
# WORKDIR /var/task

# # Copy the application code and dependencies file
# COPY app.py requirements.txt ./

# # Install dependencies in the working directory
# RUN python3.11 -m pip install --no-cache-dir -r requirements.txt -t /var/task

# # Clean up pip cache to reduce image size
# RUN rm -rf /root/.cache/pip

# # Define the Lambda function handler
# CMD ["app.lambda_handler"]

###################################################################################################
#optimizing image

# Stage 1: Build environment
# FROM python:3.11-slim AS build-env

# WORKDIR /app

# # Install dependencies and application code
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libatlas-base-dev gfortran && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY app.py .

# # Stage 2: Runtime environment
# FROM python:3.11-slim

# WORKDIR /app

# # Copy built files from the first stage
# COPY --from=build-env /app /app

# CMD ["awslambdaric", "app.lambda_handler"]

##########################################################################
# lambda base image

FROM public.ecr.aws/lambda/python:3.11

WORKDIR /var/task

COPY app.py .
COPY requirements.txt .

# Install dependencies
RUN python3.11 -m pip install --no-cache-dir -r requirements.txt -t /var/task
#RUN pip install --no-cache-dir -r requirements.txt

# Clean up pip cache to reduce image size
RUN rm -rf /root/.cache/pip

CMD ["app.lambda_handler"]
