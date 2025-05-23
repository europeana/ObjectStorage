package eu.europeana.features;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import eu.europeana.exception.S3ObjectStorageException;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Client for accessing objects stored on Amazon or IBM S3 service.
 * Created by jeroen on 14-12-16; adapted to IBM Cloud S3 by Luthien, Jan 18
 * Completely revised June 2024 by Patrick Ehlert
 */
public class S3ObjectStorageClient {

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClient.class);

    private static final String ERROR_MSG_RETRIEVE  = "Error retrieving storage object ";

    private final AmazonS3 s3Client;
    private final String bucketName;
    private boolean isIbmCloud = false;


    /**
     * Creates a new S3 client for Amazon S3
     * @param clientKey client key
     * @param secretKey client secret
     * @param region bucket region
     * @param bucketName bucket name
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName) {
       this(clientKey, secretKey, region, bucketName, (ClientConfiguration) null);
    }

    /**
     * Creates a new S3 client for Amazon S3
     * @param clientKey client key
     * @param secretKey client secret
     * @param region bucket region
     * @param bucketName bucket name
     * @param clientConfiguration optional, can be used to set a validateAfterInactivity parameter for example (see also EA-1891)
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, ClientConfiguration clientConfiguration) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(clientKey, secretKey)))
                .withRegion(region);
        if (clientConfiguration != null) {
            builder.withClientConfiguration(clientConfiguration);
        }
        s3Client = builder.build();
        this.bucketName = bucketName;
        LOG.info("Connected to Amazon S3 bucket {}, region {} ", bucketName, region);
    }

    /**
     * Create a new S3 client for IBM Cloud. Calling this constructor sets the boolean isIbmCloud to true; it is used to
     * switch to the correct way of constructing the Object URI when using resource path addressing (used by IBM Cloud)
     * instead of virtual host addressing (default usage with Amazon S3).
     * Also note that the region parameter is superfluous, but we will maintain it for now in order to be able to
     * overload the constructor (using 5 Strings, hence different from the other two)
     * @param clientKey client key
     * @param secretKey client secret
     * @param region bucket region
     * @param bucketName bucket name
     * @param endpoint endpoint to use
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, String endpoint) {
        this(clientKey, secretKey, region, bucketName, endpoint, null);
    }

    /**
     * Creates a new S3 client for Amazon S3
     * @param clientKey client key
     * @param secretKey client secret
     * @param region bucket region
     * @param bucketName bucket name
     * @param endpoint endpoint to use
     * @param clientConfiguration optional, can be used to set a validateAfterInactivity parameter for example (see also EA-1891)
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, String endpoint,
                                 ClientConfiguration clientConfiguration) {
        System.setProperty("com.amazonaws.sdk.disableDNSBuckets", "True");

        BasicAWSCredentials creds = new BasicAWSCredentials(clientKey, secretKey);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        if (clientConfiguration != null) {
            builder.withClientConfiguration(clientConfiguration);
        }
        s3Client = builder.build();
        this.bucketName = bucketName;
        isIbmCloud = true;
        LOG.info("Connected to IBM Cloud S3 bucket {}, region {} ", bucketName, region);
    }

    /**
     *  @return the name of this object storage provider
     */
    public String getServiceName() {
        return (isIbmCloud ? "IBM Cloud S3" : "Amazon S3");
    }

    /**
     * @return the name of the bucket that is being used
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * @return a list of all buckets
     */
    public List<Bucket> listBuckets() {
        return s3Client.listBuckets();
    }

    /**
     * Return a ListObjectsV2Result with summary information of all objects stored in the bucket (1000 results
     * per batch).
     * Note that you'll need to get the continuationToken to see if there are more results
     * @param continuationToken token to get next batch of objects (provide null for first request)
     * @return ListObjectsV2Result
     */
    public ListObjectsV2Result listAll(String continuationToken) {
        return listAll(continuationToken, null);
    }

    /**
     * Return a ListObjectsV2Result with summary information of all objects stored in the bucket with
     * maxPageSize results per page/batch
     * Note that you'll need to get the continuationToken to see if there are more results
     * @param continuationToken token to get next batch of objects (provide null for first request)
     * @param maxPageSize maximum number of results returned per batch/page
     * @return ListObjectsV2Result
     */
    public ListObjectsV2Result listAll(String continuationToken, Integer maxPageSize) {
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                .withBucketName(this.bucketName)
                .withContinuationToken(continuationToken);
        if (maxPageSize != null) {
            listObjectsV2Request.setMaxKeys(maxPageSize);
        }
        return s3Client.listObjectsV2(listObjectsV2Request);
    }

    /**
     * Check if an object with the provided id exists
     * @param id id of the object to check
     * @return true if it exists, false if it does not exist
     */
    public boolean isObjectAvailable(String id) {
        return s3Client.doesObjectExist(bucketName, id);
    }

    /**
     * Creates a new object or updates an existing object in the S3 bucket
     * @param id id of the object to create
     * @param inputStream object to create as an inputstream
     * @param objectMetadata metadata of the object to store
     * @return the eTag of the object as created by S3 storage
     */
    public String putObject(String id, InputStream inputStream, ObjectMetadata objectMetadata) {
        PutObjectResult putObjectResult = s3Client.putObject(new PutObjectRequest(bucketName, id, inputStream, objectMetadata));
        return (putObjectResult == null ? null : putObjectResult.getETag());
    }

    /**
     * Creates a new object or updates an existing object in the S3 bucket
     * @param id id of the object to create
     * @param inputStream object to create as an inputstream
     * @param contentType content-type of the object
     * @param contentLength content-length of the object
     * @param md5 MD5 hash of the object
     * @return the eTag of the object as created by S3 storage
     */
    public String putObject(String id, InputStream inputStream, String contentType, int contentLength, String md5) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(contentType);
        objectMetadata.setContentLength(contentLength);
        objectMetadata.setContentMD5(md5);

        return putObject(id, inputStream, objectMetadata);
    }

    /**
     * Creates a new object (containing text) in the S3 bucket.
     * @param id id of the object to create
     * @param textContents the string to store
     * @return eTag of the save object
     */
    public String putObject(String id, String textContents) {
        byte[] data = textContents.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata objectMetadata = MetadataUtils.generateObjectMetadata(data);

        return putObject(id, new ByteArrayInputStream(data), objectMetadata);
    }

    /**
     * Retrieve an S3Object from the bucket. Note that this doesn't download the actual object yet, but will provide
     * the object metadata as well as a stream that you can use to download the object. Make sure you close the object
     * when you're done!
     * @param id the id of the object to retrieve
     * @return S3Object, null if the object was not found
     * @throws S3ObjectStorageException if there was an error retrieving the object
     */
    public S3Object getObject(String id) {
        try {
            return s3Client.getObject(bucketName, id);
        } catch (AmazonS3Exception e) {
            if (!is404Exception(e)) {
                throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
            }
        }
        return null;
    }

    /**
     * Retrieve the S3 object as a stream. Make sure to close the stream when you're done!
     * @param id the id of the object to retrieve
     * @return InputStream to the downloaded object, null if the object was not found
     */
    public InputStream getObjectStream(String id) {
        try {
            S3Object object = s3Client.getObject(bucketName, id);
            return object.getObjectContent().getDelegateStream();
        } catch (AmazonS3Exception e) {
            if (!is404Exception(e)) {
                throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
            }
        }
        return null;
    }

    /**
     * Retrieve the actual S3 object as a byte array. Note that this doesn't download any object metadata. Also this
     * may not be the most efficient way of retrieving the data as it loads the entire object into memory.
     * @param id the id of the object to retrieve
     * @return array of bytes containing the downloaded object, empty array if the object was not found
     * @throws S3ObjectStorageException if there was an error retrieving the object content
     */
    public byte[] getObjectContent(String id) {
        try (S3Object object = s3Client.getObject(bucketName, id)) {
            return object.getObjectContent().readAllBytes();
        } catch (AmazonS3Exception | IOException e) {
            if (!is404Exception(e)) {
                throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
            }
        }
        return new byte[0];
    }

    /**
     * Retrieves only the metadata of an object
     * @param id the id of the object for which to retrieve the metadata
     * @return object metadata, null if the object was not found
     * @throws S3ObjectStorageException if there was an error retrieving the metadata
     */
    public ObjectMetadata getObjectMetadata(String id) {
        try {
            return s3Client.getObjectMetadata(bucketName, id);
        } catch (AmazonS3Exception e) {
            if (!is404Exception(e)) {
                throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
            }
        }
        return null;
    }

    /**
     * Deletes an object if it is present in the bucket
     * @param id the id of the object that should be deleted
     * @throws S3ObjectStorageException if there was an error deleting the object
     */
    public void deleteObject(String id) {
        try {
            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, id);
            s3Client.deleteObject(deleteObjectRequest);
        } catch (AmazonS3Exception e) {
            throw new S3ObjectStorageException("Error deleting object " + id, e);
        }
    }

    /**
     * Terminate all connections and shut down this client
     */
    public void close() {
        LOG.info("Shutting down connections to {}...", this.getServiceName());
        ((AmazonS3Client) s3Client).shutdown();
        // see also https://stackoverflow.com/questions/18069042/spring-mvc-webapp-schedule-java-sdk-http-connection-reaper-failed-to-stop
        com.amazonaws.http.IdleConnectionReaper.shutdown();
    }

    private boolean is404Exception(Exception e) {
        return (e instanceof AmazonServiceException ase) && (ase.getStatusCode() == HttpStatus.SC_NOT_FOUND);
    }

}
