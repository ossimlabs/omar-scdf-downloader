package io.ossim.omar.scdf.downloader

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
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider;


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
	OmarScdfDownloaderApplication(){
	}

	/**
	 * ResouceLoader used to access the s3 bucket objects
	 */
	@Autowired
	private ResourceLoader resourceLoader

	/**
	 * Provides a URI for the s3
	 */
	@Autowired
	private ResourcePatternResolver resourcePatternResolver

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
	@StreamListener(Processor.INPUT) @SendTo(Processor.OUTPUT)
	final String download(final Message<?> message){

		if(logger.isDebugEnabled()){
			logger.debug("Message received: ${message}")
		}

		final def parsedJson = new JsonSlurper().parseText(message.payload)

		// The list of files successfully downloaded
		final ArrayList<String> filesDownloaded = new ArrayList<String>()

		System.out.println("secretkey" + secretKey + "\naccessKey" + accessKey + "\nfilepath")
		BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
		s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).build();


		// Local storage vars for the json iteration
		String s3Bucket
		String s3Filename
		File localFile
		ObjectMetadata object

		// Loop through each received JSON file and download
		parsedJson.files.each { file ->

			s3Bucket = file.bucket
			s3Filename = file.filename

			// Create the file handle
			localFile = new File(filepath+s3Filename)

			// Download the file from S3
			object = s3Client.getObject(new GetObjectRequest(s3Bucket, s3Filename), localFile);

			// Add the file to the list of successful downloads
			filesDownloaded = s3Filename
		}

        JsonBuilder filesDownloadedJson = new JsonBuilder()

		filesDownloadedJson(files: filesDownloaded)

		if(logger.isDebugEnabled()){
			logger.debug("filesDownloadedJson: ${filesDownloadedJson}")
		}

		return filesDownloadedJson.toString()
	}
}