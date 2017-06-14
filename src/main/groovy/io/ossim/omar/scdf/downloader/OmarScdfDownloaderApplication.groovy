package io.ossim.omar.scdf.downloader

import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder


/**
 * Created by  on 5/31/2017
 */

@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfDownloaderApplication {

    /**
     * The application logger
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass())

    /**
     * Filepath passed in from application.properties
     */
    @Value('${filepath}')
    String filepath

    /**
     * AWS access key
     */
    @Value('${cloud.aws.credentials.accessKey}')
    String accessKey

    /**
     * AWS secret key
     */
    @Value('${cloud.aws.credentials.secretKey}')
    String secretKey

    /**
     * The client used to connect to S3 for downloading files
     */
    AmazonS3Client s3Client

    /**
     * Constructor
     */
    OmarScdfDownloaderApplication() {
    }


    /**
     * The main entry point of the SCDF Downloader application.
     * @param args
     */
    static void main(String[] args) {
        SpringApplication.run OmarScdfDownloaderApplication, args
    }

    /**
     * Receives a message from the SCDF aggregator, downloads the files in the message
     * and puts them in the filepath on the SCDF server
     * @param message The message object from the SCDF aggregrator (in JSON)
     * @return a JSON message of the files downloaded
     */
    @StreamListener(Processor.INPUT)
    @SendTo(Processor.OUTPUT)
    final String download(final Message<?> message) {

		println "Message payload: ${message.payload}\n"

		def x = message.payload.equals( "null")
		println "x: ${x} \n"


		if ( !x ) {
			def parsedJson = new JsonSlurper().parseText(message.payload)

			// The list of files successfully downloaded
			final ArrayList<String> filesDownloaded = new ArrayList<String>()

			final BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey)
			s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).build()

			int i


			for(i=0;i<parsedJson.files.size();i++ )
			{
				println "parsedJson.files.size()" + parsedJson.files.size()
				try {
					// Download the file from S3
					println "got to try"
					println "bucket i = " + i + " " + parsedJson.files[i].bucket.toString()
					println "file i = " + i + " " + parsedJson.files[i].filename.toString()
					File file = new File(filepath + parsedJson.files[i].filename.toString())
					s3Client.getObject(new GetObjectRequest(parsedJson.files[i].bucket.toString(), parsedJson.files[i].filename.toString()), localFile)
					// Add the file to the list of successful downloads
					println "parsedJson.files[" + i + "].bucket.toString()" + parsedJson.files[i].bucket.toString()
					println "parsedJson.files[" + i + "].filename.toString()" + parsedJson.files[i].filename.toString()
					filesDownloaded.add(parsedJson.files[i].filename.toString())
				} catch (SdkClientException e) {
					println "sdkclientexception"
				} catch (AmazonServiceException e) {
					println "amazonservice exception"
				} catch (Exception e) {
					println "exception" + e
				}
			}

		}

		// Create the output JSON
		JsonBuilder filesDownloadedJson = new JsonBuilder()
		filesDownloadedJson(files: filesDownloaded)
		if (logger.isDebugEnabled()) {
			logger.debug("filesDownloadedJson: ${filesDownloadedJson}")
		}
		return filesDownloadedJson.toString()
	}
}