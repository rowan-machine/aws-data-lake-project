# Use the official Spark image with Python support
FROM apache/spark-py:latest

# Install necessary packages
USER root
RUN apt-get update \
    && apt-get install -y python3-pip python3-setuptools unzip curl
RUN pip3 install --upgrade pip wheel
RUN pip3 install jupyter pydeequ boto3 pyspark

# Copy AWS Glue libraries
COPY aws-glue-libs/dist/aws_glue_libs-4.0.0-py3.8.egg /opt/glue/aws_glue_libs-4.0.0-py3.8.egg

# Create directory for jars
RUN mkdir -p /opt/glue/jars

# Download the Iceberg runtime jar using curl
RUN wget -O /opt/glue/jars/iceberg-spark3-runtime-0.12.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark3-runtime/0.12.0/iceberg-spark3-runtime-0.12.0.jar

# Set environment variables to include the AWS Glue libraries
ENV PYTHONPATH="$PYTHONPATH:/opt/glue/aws_glue_libs-4.0.0-py3.8.egg"

# Set the working directory
WORKDIR /opt/glue

# Expose port for Jupyter Notebook
EXPOSE 8888

# Generate Jupyter Notebook configuration file
RUN jupyter notebook --generate-config

# Install the Jupyter notebook package
RUN pip3 install notebook

# Set the Jupyter Notebook password
RUN python3 -c "from jupyter_server.auth import passwd; print(passwd('1234'))" > /tmp/gen_passwd.txt && \
    PASSWORD_HASH=$(cat /tmp/gen_passwd.txt) && \
    echo "c.NotebookApp.password = u'$PASSWORD_HASH'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /root/.jupyter/jupyter_notebook_config.py

# Copy the script to the container
COPY glue/scripts/ /opt/glue/scripts/

# Copy Iceberg and PyDeequ JAR files
COPY jars/ /opt/glue/jars/

# Run Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
