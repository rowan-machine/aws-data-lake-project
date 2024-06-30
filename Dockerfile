# Use the official Spark image with Python support
FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01

# Install required packages
USER root
RUN yum update -y && \
    yum install -y \
    python3-pip \
    python3-setuptools && \
    rm -rf /var/lib/apt/lists/*

# Install python packages
RUN pip3 install --upgrade pip wheel
RUN pip3 install jupyter awscli boto3 pyspark pandas notebook great_expectations

# Create directory for Glue libraries and jars
RUN mkdir -p /opt/glue/jars /var/log/glue

# Copy JAR files
COPY  jars/aws-java-sdk-bundle-1.11.375.jar /opt/glue/jars/
COPY  jars/hadoop-aws-3.2.0.jar /opt/glue/jars/

# Set environment variables to include the AWS Glue libraries
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH

# Copy the logging configuration file
COPY logging.conf /opt/glue/logging.conf

# Set the working directory
WORKDIR /opt

# Copy the local files into the container
COPY . /opt

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
