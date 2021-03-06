#  omar-scdf-downloader

## Pre-Req to installing

1. Have a SCDF server set up 
2. Have a server with the following 3 microservices running: omar-scdf-server, omar-scdf-zookeeper, and omar-scdf-kafka

## Installation/running process

1)  Run the following command.
java -jar spring-cloud-dataflow-shell.jar
2) At the prompt, enter the following (if you have a SCDF server set up)
dataflow config server <SCDF server>:9393 --username <username> --password <password>
3) run this command to register the app: app register --name omar-scdf-downloader --type processor --uri <jar location>
4) If you have additional apps to connect, use the following command to make a stream:
stream create --name <something> --definition "omar-scdf-downloader | <otherapp> | log".  You would then deploy that stream by doing the following command: stream deploy --name <something> --properties "app.omar-scdf-downloader.<property>=<value>" for each property.  If you want the properties below, you don't need the --properties flag.


## Sample application.properties file
```
spring.cloud.stream.bindings.input.destination=sqs-aggregated
spring.cloud.stream.bindings.output.destination=files-downloaded
spring.cloud.stream.bindings.output.content.type=application/json
server.port=0

logging.level.org.springframework.web=ERROR
logging.level.io.ossim.omar.scdf.downloader=DEBUG

filepath=/data

# Don't use Cloud Formation
cloud.aws.stack.auto=false
cloud.aws.credentials.instanceProfile=true
cloud.aws.region.auto=true

#cloud.aws.credentials.accessKey=a
#cloud.aws.credentials.secretKey=b
```