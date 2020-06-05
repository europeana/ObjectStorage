package eu.europeana.features;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.IOUtils;
import eu.europeana.domain.ContentValidationException;
import eu.europeana.domain.ObjectMetadata;
import eu.europeana.domain.ObjectStorageClientException;
import eu.europeana.domain.StorageObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Client for accessing objects stored on (Amazon or Bluemix or ...) S3 service.
 * Created by jeroen on 14-12-16; adapted to IBM Bluemix S3 by Luthien, Jan 18
 */
public class S3ObjectStorageClient implements ObjectStorageClient {

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClient.class);

    private static final String ERROR_MSG_RETRIEVE = "Error retrieving storage object ";

    private AmazonS3 client;
    private String bucketName;
    private boolean isIbmCloud = false;

    /**
     * Create a new S3 client for Amazon S3
     * @param clientKey
     * @param secretKey
     * @param region
     * @param bucketName
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName) {
        AWSCredentials credentials = new BasicAWSCredentials(clientKey, secretKey);
        // setting client configuration
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withValidateAfterInactivityMillis(20000);
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(clientConfiguration)
                .withRegion(region)
                .build();
        this.bucketName = bucketName;
        LOG.info("Connected to Amazon S3 bucket {}, region {} ", bucketName, region);
    }

    /**
     * Create a new S3 client for IBM Cloud/Bluemix. Calling this constructor sets the boolean isBlueMix
     * to true; it is used to switch to the correct way of constructing the Object URI when using resource path
     * addressing (used by Bluemix) instead of virtual host addressing (default usage with Amazon S3).
     * Also note that the region parameter is superfluous, but I will maintain it for now in order to be able to
     * overload the constructor (using 5 Strings, hence different from the other two)
     * @param clientKey
     * @param secretKey
     * @param region
     * @param bucketName
     * @param endpoint
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName, String endpoint) {
        System.setProperty("com.amazonaws.sdk.disableDNSBuckets", "True");
        S3ClientOptions.Builder optionsBuilder = S3ClientOptions.builder().setPathStyleAccess(true);
        client = new AmazonS3Client(new BasicAWSCredentials(clientKey, secretKey));
        client.setS3ClientOptions(optionsBuilder.build());
        client.setEndpoint(endpoint);
        this.bucketName = bucketName;
        isIbmCloud = true;
        LOG.info("Connected to IBM Cloud S3 bucket {}, region {}", bucketName, region);
    }

    /**
     * Create a new S3 client and specify an endpoint and options
     * @param clientKey
     * @param secretKey
     * @param bucketName
     * @param endpoint
     * @param s3ClientOptions
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String bucketName, String endpoint, S3ClientOptions s3ClientOptions) {
        client = new AmazonS3Client(new BasicAWSCredentials(clientKey, secretKey));
        client.setS3ClientOptions(s3ClientOptions);
        client.setEndpoint(endpoint);
        this.bucketName = bucketName;
        LOG.info("Connected to S3 bucket {}", bucketName);
    }

    /**
     * @see ObjectStorageClient#getName()
     */
    @Override
    public String getName() {
        return (isIbmCloud ? "IBM Cloud S3" : "Amazon S3");
    }

    /**
     * @see ObjectStorageClient#getBucketName()
     */
    @Override
    public String getBucketName() {
        return bucketName;
    }

    /**
     * @see ObjectStorageClient#list()
     */
    @Override
    public List<StorageObject> list() {
        ObjectListing objectListing = client.listObjects(bucketName);
        List<S3ObjectSummary> results = objectListing.getObjectSummaries();
        ArrayList<StorageObject> storageObjects = new ArrayList<>();
        for (S3ObjectSummary so : results) {
            storageObjects.add(toStorageObject(so));
        }
        while (objectListing.isTruncated()) {
            objectListing = client.listNextBatchOfObjects(objectListing);
            for (S3ObjectSummary so : objectListing.getObjectSummaries()) {
                storageObjects.add(toStorageObject(so));
            }
        }
        return storageObjects;
    }

    /**
     * @see ObjectStorageClient#isAvailable(String)
     */
    @Override
    public boolean isAvailable(String id) {
        return client.doesObjectExist(bucketName, id);
    }

    public void setEndpoint(String endpoint) {
        client.setEndpoint(endpoint);
    }

    private StorageObject toStorageObject(S3ObjectSummary so) {
        URI uri = getUri(so.getKey());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setLastModified(so.getLastModified());
        metadata.setETag(so.getETag());
        metadata.setContentLength(so.getSize());
        return new StorageObject(so.getKey(), uri, metadata, null);
    }

    private URI getUri(String key) {
        if (isIbmCloud) {
            return URI.create(client.getUrl(bucketName, key).toString());
        } else {
            String bucketLocation = client.getRegionName();
            return URI.create(bucketLocation + "/" + key);
        }
    }

    /**
     * @see ObjectStorageClient#put(StorageObject)
     */
    @Override
    public String put(StorageObject storageObject) {
        com.amazonaws.services.s3.model.ObjectMetadata metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
        metadata.setContentType(storageObject.getMetadata().getContentType());
        metadata.setContentLength(storageObject.getMetadata().getContentLength());
        metadata.setContentMD5(storageObject.getMetadata().getContentMD5());

        PutObjectResult putObjectResult = null;
        try (InputStream inputStream = storageObject.getPayload().openStream()) {
            putObjectResult = client.putObject(new PutObjectRequest(bucketName, storageObject.getName(), inputStream, checkMetaData(metadata)));
        } catch (IOException e) {
            LOG.error("Error storing object "+storageObject.getName(), e);
        }
        return (putObjectResult == null ? null : putObjectResult.getETag());
    }

    /**
     * @see ObjectStorageClient#put(String, Payload)
     */
    @Override
    @SuppressWarnings("squid:S2070") // ignore SonarQube MD5 warnings because we have no choice but to use that
    public String put(String key, Payload value) {
        com.amazonaws.services.s3.model.ObjectMetadata metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
        byte[] content = new byte[0];

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (DigestInputStream dis = new DigestInputStream(value.openStream(), md)) {
                content = IOUtils.toByteArray(dis);
                metadata.setContentMD5(BinaryUtils.toBase64(md.digest()));
            }
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Cannot calculate MD5 hash of because no MD5 algorithm was found",e );
        } catch (IOException e) {
            LOG.error("Error reading payload for key "+key, e);
        }
        Integer intLength = Integer.valueOf(content.length);
        metadata.setContentLength(intLength.longValue());

        // TODO figure out if we really need to create a new stream here or can make it more efficient by using the previously used stream
        PutObjectResult putObjectResult = null;
        try (InputStream is = new ByteArrayInputStream(content) ){
            putObjectResult = client.putObject(new PutObjectRequest(bucketName, key, is, checkMetaData(metadata)));
        } catch (IOException e) {
            LOG.error("Error storing object with key "+key, e);
        }

        return (putObjectResult == null ? null : putObjectResult.getETag());
    }

    private com.amazonaws.services.s3.model.ObjectMetadata checkMetaData(com.amazonaws.services.s3.model.ObjectMetadata metadata) {
        if (metadata.getContentLength() == 0L) {
            throw new ObjectStorageClientException("The metadata ContentLength is mandatory");
        }
        return metadata;
    }

    /**
     * @see ObjectStorageClient#getWithoutBody(String)
     */
    @Override
    public Optional<StorageObject> getWithoutBody(String objectName) {
        try {
            return Optional.ofNullable(retrieveAsStorageObject(objectName, false, false));
        } catch (ContentValidationException | AmazonS3Exception e) {
            throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE + objectName + " without body", e);
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#get(String)
     */
    @Override
    public Optional<StorageObject> get(String objectName) {
        try {
            return Optional.ofNullable(retrieveAsStorageObject(objectName, true, false));
        } catch (ContentValidationException | AmazonS3Exception e) {
            throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE + objectName, e);
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#get(String, boolean)
     */
    @Override
    public Optional<StorageObject> get(String objectName, boolean verify) throws ContentValidationException {
        try {
            return Optional.ofNullable(retrieveAsStorageObject(objectName, true, verify));
        } catch (AmazonS3Exception ex) {
            throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE + objectName, ex);
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#getContent(String)
     */
    @Override
    public byte[] getContent(String objectName) {
        byte[] result = new byte[0];
        try {
            result = retrieveAsBytes(objectName);
        } catch (AmazonS3Exception ex) {
            if (!is404Exception(ex)) {
                throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName, ex);
            }
        }
        return result;
    }

    private ObjectMetadata getObjectMetaData(String id) {
        com.amazonaws.services.s3.model.ObjectMetadata s3data = client.getObjectMetadata(bucketName, id);
        return new ObjectMetadata(s3data.getRawMetadata());
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#getMetaData(String)
     */
    @Override
    public ObjectMetadata getMetaData(String id) {
        try {
            return getObjectMetaData(id);
        } catch (AmazonS3Exception ex) {
            if (!is404Exception(ex)) {
                throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE + id, ex);
            }
        }
        return null;
    }

    /**
     * @see ObjectStorageClient#delete(String)
     */
    @Override
    public void delete(String objectName) {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, objectName);
        client.deleteObject(deleteObjectRequest);
    }

    /**
     * @see ObjectStorageClient#close()
     */
    @Override
    public void close() {
        LOG.info("Shutting down connections to {} ...", this.getName());
        ((AmazonS3Client) client).shutdown();
        // see also https://stackoverflow.com/questions/18069042/spring-mvc-webapp-schedule-java-sdk-http-connection-reaper-failed-to-stop
        com.amazonaws.http.IdleConnectionReaper.shutdown();
    }

    /**
     * Retrieve an object and return all information as a {@link StorageObject}}
     * @param id
     * @param getContent, if false then only metadata is retrieved
     * @param verify if true then the MD5 hash of the content will be verified to check if it was downloaded correctly
     * @return null if the object was not found (404 exception)
     */
    private StorageObject retrieveAsStorageObject(String id, boolean getContent, boolean verify) throws ContentValidationException {
        StorageObject result = null;
        if (getContent) {
            try (S3Object object = client.getObject(bucketName, id)) {
                result = getContent(object, verify);
            } catch (IOException e) {
                LOG.error("Error reading object content", e);
            } catch (RuntimeException e) {
                if (!is404Exception(e)) {
                    LOG.error("Error reading object content ", e);
                }
            }
        } else {
            try {
                ObjectMetadata metadata = getObjectMetaData(id);
                result = new StorageObject(id, getUri(id), metadata, null);
            } catch (RuntimeException e) {
                if (!is404Exception(e)) {
                    LOG.error("Error reading object content ", e);
                }
            }
        }
        return result;
    }

    private StorageObject getContent(S3Object object, boolean verify) throws IOException, ContentValidationException {
        ObjectMetadata objectMetadata = new ObjectMetadata(object.getObjectMetadata().getRawMetadata());
        ByteArrayPayload content;
        try (S3ObjectInputStream contentStream = object.getObjectContent()) {
            if (verify) {
                content = readAndVerifyContent(contentStream, BinaryUtils.fromHex(objectMetadata.getETag()));
            } else {
                content = new ByteArrayPayload(IOUtils.toByteArray(contentStream));
            }
            content.close();
        }
        return new StorageObject(object.getKey(), getUri(object.getKey()), objectMetadata, content);
    }

    private boolean is404Exception(Exception e) {
        return (e instanceof AmazonServiceException) &&
                (((AmazonServiceException) e).getStatusCode() == HttpStatus.SC_NOT_FOUND);
    }

    /**
     * Add a messageDigest to the provided stream and calculate the hash when reading is done.
     * Then the hash is compared to the hash on the server.
     * @param contentStream
     * @param serverSideHash
     * @return read and verified content from the stream
     * @throws ContentValidationException, IOException
     */
    @SuppressWarnings("squid:S2070") // ignore SonarQube MD5 warnings because we have no choice but to use that
    private ByteArrayPayload readAndVerifyContent(S3ObjectInputStream contentStream, byte[] serverSideHash) throws ContentValidationException, IOException {
        ByteArrayPayload result = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (DigestInputStream dis = new DigestInputStream(contentStream, md)) {
                result = new ByteArrayPayload(IOUtils.toByteArray(dis));
                result.close();
                byte[] streamHash = md.digest();
                if (streamHash == null || serverSideHash == null || !Arrays.equals(streamHash, serverSideHash)) {
                    throw new ContentValidationException("Error comparing retrieved content hash with server hash (content = "
                            +Arrays.toString(streamHash)+ ", server = "+Arrays.toString(serverSideHash));
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new ContentValidationException("Cannot verify MD5 hash of downloaded content because no MD5 algorithm was found",e );
        }
        return result;
    }

    /**
     * Retrieve an object and return it's content only as a byte[].
     * Note that this is much faster than the retrieveStorageObject()
     * @param id
     * @return
     */
    private byte[] retrieveAsBytes(String id) {
        try (S3Object object = client.getObject(bucketName, id)) {
            return IOUtils.toByteArray(object.getObjectContent());
        } catch (IOException e) {
            LOG.error(ERROR_MSG_RETRIEVE +id, e);
        }
        return new byte[0];
    }

    /**
     * Create a new bucket with the provided name and switch to this new bucket
     * @param bucketName
     * @return the newly created bucket
     */
    public Bucket createBucket(String bucketName) {
        Bucket result = client.createBucket(bucketName);
        this.bucketName = bucketName;
        return result;
    }

    /**
     * @return a list of all buckets
     */
    public List<Bucket> listBuckets() {
        return client.listBuckets();
    }

    /**
     * Delete a bucket (requires admin privileges)
     * @param bucket
     */
    public void deleteBucket(String bucket) {
        client.deleteBucket(bucket);
    }

    public void setS3ClientOptions(S3ClientOptions s3ClientOptions) {
        client.setS3ClientOptions(s3ClientOptions);
    }
}
