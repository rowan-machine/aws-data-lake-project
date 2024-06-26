# Use the official Spark image with Python support
FROM apache/spark-py:latest

# Install required packages
USER root
RUN apt-get update \
    && apt-get install -y \
        python3-pip \
        python3-setuptools \
        curl \
        wget \
        unzip \ 
        && rm -rf /var/lib/apt/lists/*

# Install python packages
RUN pip3 install --upgrade pip wheel

# Create requirements.txt file with pinned versions
COPY requirements.txt .

# Clear pip cache to prevent mismatches
RUN pip3 cache purge

# Install packages from requirements.txt
RUN pip3 install --no-cache-dir --no-cache -r requirements.txt

# Create directory for Glue libraries and jars
RUN mkdir -p /opt/glue/jars /var/log/glue

# Copy necessary files
COPY aws-glue-libs/ /opt/glue
COPY glue/scripts /opt/glue/scripts

# Copy AWS Glue libraries
COPY aws-glue-libs/aws_glue_libs-4.0.0-py3.8.egg /opt/glue/aws_glue_libs-4.0.0-py3.8.egg

# Copy PyDeequ JAR file
COPY jars/deequ-1.0.5.jar /opt/glue/jars/deequ-1.0.5.jar

# Install the Deequ JAR
RUN pip install pydeequ

#Copy Iceberg runtime jar
COPY jars/iceberg-spark3-runtime-0.12.0.jar /opt/glue/jars/iceberg-spark3-runtime-0.12.0.jar

# Download additional JARs using wget
RUN wget -O /opt/glue/jars/hadoop-aws-3.2.0.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    wget -O /opt/glue/jars/aws-java-sdk-bundle-1.11.375.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

# Set environment variables to include the AWS Glue libraries
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH="$PYTHONPATH:/opt/glue/aws_glue_libs-4.0.0-py3.8.egg"

# Copy the logging configuration file
COPY logging.conf /opt/glue/logging.conf

# Set the working directory
WORKDIR /opt/glue/scripts

# Expose port for Jupyter Notebook
EXPOSE 8888

# Generate Jupyter Notebook configuration file
RUN jupyter notebook --generate-config

# Set the Jupyter Notebook password
RUN python3 -c "from jupyter_server.auth import passwd; print(passwd('1234'))" > /tmp/gen_passwd.txt && \
    PASSWORD_HASH=$(cat /tmp/gen_passwd.txt) && \
    echo "c.NotebookApp.password = u'$PASSWORD_HASH'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /root/.jupyter/jupyter_notebook_config.py

# Copy the script to the container
COPY glue/scripts/ /opt/glue/scripts/

# Copy JAR files
COPY jars/ /opt/glue/jars/

# Run Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
