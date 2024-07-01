FROM bitnami/spark:3.5.1

# Set the working directory
WORKDIR /app

# Copy the requirements.txt file
COPY requirements.txt /app/requirements.txt

# Install the necessary dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the application files
COPY . /app

# Create an empty data and logs directory
RUN mkdir -p /app/data
RUN mkdir -p /app/logs

# Ensure the shell script is executable
USER root
RUN chmod +x /app/run_all.sh

# Run the shell script
CMD ["/app/run_all.sh"]