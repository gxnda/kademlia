FROM ubuntu:latest
LABEL authors="Gabriel Lancaster-West"

# Stops input when running apt-get
ENV DEBIAN_FRONTEND=noninteractive


# Set the working directory
WORKDIR /app

# Install python and pip

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-tk

# Copy the directory with source code in to /app
COPY . /app

# Set the environment variable
ENV PORT 7124

# Expose the port
EXPOSE $PORT

# Install dependancies
RUN pip install -r requirements.txt

# Run the application
CMD python3 GUI.py --port $PORT