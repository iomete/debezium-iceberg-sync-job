FROM iomete.azurecr.io/iomete/spark:3.5.5-v3

# Remove MySQL connector from base image which is conflicting with the one deployed by Quarkus
USER root
RUN rm -rf /opt/spark/jars/mysql-connector-java-*.jar

# Add spark custom log4j2 configuration
COPY local-docker-run/conf/log4j2.properties /etc/configs/log4j2.properties

COPY build/*-runner.jar /opt/spark/jars/

# Set the spark user as the default user back again
USER ${spark_uid}
