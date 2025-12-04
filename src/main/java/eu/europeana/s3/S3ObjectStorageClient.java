package eu.europeana.s3;

import eu.europeana.s3.exception.S3ObjectStorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client for accessing objects stored on Amazon or IBM S3 service.
 * Created by jeroen on 14-12-16; adapted to IBM Cloud S3 by Luthien, Jan 18
 * Completely revised June 2024 and Nov 2025 by Patrick Ehlert
 */
public class S3ObjectStorageClient {

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClient.class);

    private static final String ERROR_MSG_MISSING_ENDPOINT_SCHEME = "Endpoint scheme is missing ";
    private static final String ERROR_MSG_RETRIEVE  = "Error retrieving storage object ";
    private static final String ERROR_MSG_CONTENT_TYPE_REQUIRED = "Setting a content-type is required!";

    private final S3Client s3Client;
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
       this(clientKey, secretKey, region, bucketName, (SdkHttpClient) null);
    }

    /**
     * Creates a new S3 client for Amazon S3
     * @param clientKey client key
     * @param secretKey client secret
     * @param region bucket region
     * @param bucketName bucket name
     * @param httpClient optional, can be used to set client properties like connectionTimeout or maxConnections
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, SdkHttpClient httpClient) {
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(clientKey, secretKey)))
                .region(Region.of(region));
        if (httpClient != null) {
            builder.httpClient(httpClient);
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
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, URI endpoint) {
        this(clientKey, secretKey, region, bucketName, endpoint, null);
    }

    /**
     * Creates a new S3 client for IBM Cloud
     * @param clientKey client key
     * @param secretKey client secret
     * @param region bucket region
     * @param bucketName bucket name
     * @param endpoint endpoint to use
     * @param httpClient optional, can be used to set client properties like connectionTimeout or maxConnections
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, URI endpoint,
                                 SdkHttpClient httpClient) {
        if (endpoint == null || StringUtils.isBlank(endpoint.getScheme())) {
            throw new S3ObjectStorageException(ERROR_MSG_MISSING_ENDPOINT_SCHEME + endpoint);
        }
        AwsBasicCredentials creds = AwsBasicCredentials.create(clientKey, secretKey);
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .forcePathStyle(true)
                .endpointOverride(endpoint)
                .region(Region.of(region));
        if (httpClient != null) {
            builder.httpClient(httpClient);
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
     * @return a list of all buckets (assuming that are not so many that we need a continuationToekn)
     */
    public List<Bucket> listBuckets() {
        return s3Client.listBuckets().buckets();
    }

    /**
     * Return a ListObjectsV2Result with summary information of all objects stored in the bucket (1000 results
     * per batch).
     * Note that you'll need to get the continuationToken to see if there are more results
     * @param continuationToken token to get next batch of objects (provide null for first request)
     * @return ListObjectsV2Result
     */
    public ListObjectsV2Response listAll(String continuationToken) {
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
    public ListObjectsV2Response listAll(String continuationToken, Integer maxPageSize) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(this.bucketName)
                .continuationToken(continuationToken)
                .build();
        if (maxPageSize != null) {
            listObjectsV2Request = listObjectsV2Request.toBuilder().maxKeys(maxPageSize).build();
        }
        return s3Client.listObjectsV2(listObjectsV2Request);
    }

    /**
     * Check if an object with the provided id exists
     * @param id id of the object to check
     * @return true if it exists, false if it does not exist
     */
    @SuppressWarnings("java:S1166") // no need to log "no such key exception"
    public boolean isObjectAvailable(String id) {
        try {
            s3Client.headObject(req -> req
                    .bucket(this.bucketName)
                    .key(id));
            return true;
        } catch (NoSuchKeyException noSuchKeyException) {
            return false;
        } catch (S3Exception e) {
            throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
        }
    }

    /**
     * Retrieves only the metadata of an object
     * @param id the id of the object for which to retrieve the metadata
     * @return object metadata, null if the object was not found
     * @throws S3ObjectStorageException if there was an error retrieving the metadata
     */
    @SuppressWarnings({"java:S1166", "java:S1168" }) // no need to log "no such key exception"
    //also we deliberately return null to indicate the object was not found.
    public Map<String, Object> getObjectMetadata(String id) {
        try {
            HeadObjectResponse response = s3Client.headObject(req -> req
                    .bucket(this.bucketName)
                    .key(id));
            return fillMetadataFromHead(response);
        } catch (NoSuchKeyException noSuchKeyException) {
            return null;
        } catch (S3Exception e) {
            throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
        }
    }

    /**
     * With AWS S3 SDKv2 fields like lastModified, contentLength, contentType etc. are now part of the response, and not
     * the metadata. So we put them in the metadata ourselves
     * Unfortunately AWS SDK duplicated metadata fields in HeadObjectResponse and GetObjectResponse, so we also had to
     * duplicate the methods to cover those responses
     */
    private Map<String, Object> fillMetadataFromHead(HeadObjectResponse response) {
        Map<String, Object> metadata = new HashMap<>(response.metadata());
        if (response.lastModified() != null) {
            metadata.put(S3Object.LAST_MODIFIED, response.lastModified());
        }
        if (response.contentLength() != null) {
            metadata.put(S3Object.CONTENT_LENGTH, response.contentLength());
        }
        if (response.contentType() != null) {
            metadata.put(S3Object.CONTENT_TYPE, response.contentType());
        }
        if (response.contentLength() != null) {
            metadata.put(S3Object.CONTENT_ENCODING, response.contentEncoding());
        }
        if (response.eTag() != null) {
            metadata.put(S3Object.ETAG, response.eTag());
        }
        if (response.versionId() != null) {
            metadata.put(S3Object.VERSION_ID, response.versionId());
        }
        return metadata;
    }

    private Map<String, Object> fillMetadataFromGet(GetObjectResponse response) {
        Map<String, Object> metadata = new HashMap<>(response.metadata());
        if (response.lastModified() != null) {
            metadata.put(S3Object.LAST_MODIFIED, response.lastModified());
        }
        if (response.contentLength() != null) {
            metadata.put(S3Object.CONTENT_LENGTH, response.contentLength());
        }
        if (response.contentType() != null) {
            metadata.put(S3Object.CONTENT_TYPE, response.contentType());
        }
        if (response.contentLength() != null) {
            metadata.put(S3Object.CONTENT_ENCODING, response.contentEncoding());
        }
        if (response.eTag() != null) {
            metadata.put(S3Object.ETAG, response.eTag());
        }
        if (response.versionId() != null) {
            metadata.put(S3Object.VERSION_ID, response.versionId());
        }
        return metadata;
    }

    /**
     * Retrieve an S3 object from the bucket as a byte array. Note that this doesn't return the object's metadata
     * @param id the id of the object to retrieve
     * @return byte array representing the object, empty array if the object was not found
     */
    @SuppressWarnings("java:S1166") // no need to log "no such key exception"
    public byte[] getObjectAsBytes(String id) {
        try {
            ResponseBytes<GetObjectResponse> response = s3Client.getObject(req -> req
                    .bucket(this.bucketName)
                    .key(id)
                    .build(), ResponseTransformer.toBytes());
            return response.asByteArray();
        } catch (NoSuchKeyException noSuchKeyException) {
            return new byte[0];
        } catch (S3Exception e) {
            throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
        }
    }

    /**
     * Retrieve an S3 object from the bucket as a byte array. Note that this doesn't return the object's metadata
     * @param id the id of the object to retrieve. Make sure to close the stream when you're done!
     * @return stream representing the object, null if the object was not found
     */
    @SuppressWarnings("java:S1166") // no need to log "no such key exception"
    public InputStream getObjectAsStream(String id) {
        try {
            return s3Client.getObject(req -> req
                    .bucket(this.bucketName)
                    .key(id));
        } catch (NoSuchKeyException noSuchKeyException) {
            return null;
        } catch (S3Exception e) {
            throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
        }
    }

    /**
     * Return an S3Object wih an input stream and a map representing the object's metadata. Make sure to close the
     * stream when you're done!
     * @param id the id of the object to retrieve
     * @return S3Object
     */
    @SuppressWarnings("java:S1166") // no need to log "no such key exception"
    public S3Object getObject(String id) {
        try {
            ResponseBytes<GetObjectResponse> response = s3Client.getObject(req -> req
                    .bucket(this.bucketName)
                    .key(id)
                    .build(), ResponseTransformer.toBytes());
            return new S3Object(id, response.asInputStream(),
                    fillMetadataFromGet(response.response()));
        } catch (NoSuchKeyException noSuchKeyException) {
            return null;
        } catch (S3Exception e) {
            throw new S3ObjectStorageException(ERROR_MSG_RETRIEVE + id, e);
        }
    }

    /**
     * Creates a new object or updates an existing object in the S3 bucket
     * @param id id of the object to create
     * @param contentType the content-type to store
     * @param contents object to create as a byte array
     * @return the eTag of the object as created by S3 storage
     */
    public String putObject(String id, String contentType, byte[] contents) {
        return putObject(id, contentType, contents, null);
    }

    /**
     * Creates a new object or updates an existing object in the S3 bucket
     * @param id id of the object to create
     * @param contentType the content-type to store
     * @param contents object to create as a byte array
     * @param metadata optional (can be null), map with metadata key-value pairs
     * @return the eTag of the object as created by S3 storage
     */
    public String putObject(String id, String contentType, byte[] contents, Map<String, String> metadata) {
        if (contentType == null) {
            // if we don't set content-type ourselves AWS SDK will set it as application/octet-stream
            throw new S3ObjectStorageException(ERROR_MSG_CONTENT_TYPE_REQUIRED);
        }
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(this.bucketName)
                .key(id)
                .metadata(metadata)
                .build();
        RequestBody requestBody = RequestBody.fromContentProvider(
                ContentStreamProvider.fromByteArray(contents), contentType);
        PutObjectResponse putObjectResult = s3Client.putObject(putObjectRequest, requestBody);
        return (putObjectResult == null ? null : putObjectResult.eTag());
    }


    /**
     * Creates a new object or updates an existing object in the S3 bucket
     * @param id id of the object to create or update
     * @param contentType the content-type to store
     * @param inputStream object to create as an input stream
     * @return the eTag of the object as created by S3 storage
     */
    public String putObject(String id, String contentType, InputStream inputStream) {
        return putObject(id, contentType, inputStream,null);
    }


    /**
     * Creates a new object or updates an existing object in the S3 bucket
     * @param id id of the object to create
     * @param contentType the content-type to store
     * @param inputStream object to create as an input stream
     * @param metadata optional (can be null), map with metadata key-value pairs
     * @return the eTag of the object as created by S3 storage
     */
    public String putObject(String id, String contentType, InputStream inputStream, Map<String, String> metadata) {
        if (contentType == null) {
            // if we don't set content-type ourselves AWS SDK will set it as application/octet-stream
            throw new S3ObjectStorageException(ERROR_MSG_CONTENT_TYPE_REQUIRED);
        }
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(this.bucketName)
                .key(id)
                .metadata(metadata)
                .build();
        RequestBody requestBody = RequestBody.fromContentProvider(
                ContentStreamProvider.fromInputStream(inputStream), contentType);
        PutObjectResponse putObjectResult = s3Client.putObject(putObjectRequest, requestBody);
        return (putObjectResult == null ? null : putObjectResult.eTag());
    }

    /**
     * Deletes an object if it is present in the bucket
     * @param id the id of the object that should be deleted
     * @throws S3ObjectStorageException if there was an error deleting the object
     */
    public void deleteObject(String id) {
        try {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(id)
                    .build();
            s3Client.deleteObject(deleteObjectRequest);
        } catch (S3Exception e) {
            throw new S3ObjectStorageException("Error deleting object " + id, e);
        }
    }

    /**
     * Terminate all connections and shut down this client
     */
    public void close() {
        LOG.info("Shutting down connections to {}...", this.getServiceName());
        s3Client.close();
    }

}
