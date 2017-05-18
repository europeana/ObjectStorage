package eu.europeana.features;

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
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Client for accessing objects stored on Amazon S3 service.
 * Created by jeroen on 14-12-16.
 */
public class S3ObjectStorageClient implements ObjectStorageClient {

    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectStorageClient.class);
    private static final String ERROR_MSG_RETRIEVE = "Error retrieving storage object ";

    private AmazonS3 client;
    private String bucketName;

    /**
     * Create a new S3 client
     * @param clientKey
     * @param secretKey
     * @param region
     * @param bucketName
     */
    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucketName) {
        AWSCredentials credentials = new BasicAWSCredentials(clientKey, secretKey);
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region).build();
        this.bucketName = bucketName;
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

    public void setEndpoint(String endpoint) {
        client.setEndpoint(endpoint);
    }

    private StorageObject toStorageObject(S3ObjectSummary so) {
        URI uri = getUri(so.getKey());
        return new StorageObject(so.getKey(), uri, so.getLastModified(), null, null);
    }

    private URI getUri(String key) {
        String bucketLocation = client.getBucketLocation(bucketName);
        return URI.create(bucketLocation + "/" + key);
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
        if (metadata.getContentLength() == 0l) {
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
            return Optional.of(retrieveAsStorageObject(objectName, Boolean.FALSE, Boolean.FALSE));
        } catch (ContentValidationException e) {
            throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName+ " without body", e);
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName+ " without body", ex);
            }
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#get(String)
     */
    @Override
    public Optional<StorageObject> get(String objectName) {
        try {
            return Optional.of(retrieveAsStorageObject(objectName, Boolean.TRUE, Boolean.FALSE));
        } catch (ContentValidationException e) {
            throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName, e);
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName, ex);
            }
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#get(String, boolean)
     */
    @Override
    public Optional<StorageObject> get(String objectName, boolean verify) throws ContentValidationException {
        try {
            return Optional.of(retrieveAsStorageObject(objectName, Boolean.TRUE, verify));
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName, ex);
            }
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
            if (ex.getStatusCode() != 404) {
                throw new ObjectStorageClientException(ERROR_MSG_RETRIEVE +objectName, ex);
            }
        }
        return result;
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
     * Retrieve an object and return all information as a {@link StorageObject}}
     * @param id
     * @param getContent if false only header information is retrieved, if true payload is retrieved as well
     * @return
     */
    private StorageObject retrieveAsStorageObject(String id, boolean getContent, boolean verify) throws ContentValidationException {
        StorageObject result = null;
        try (S3Object object = client.getObject(bucketName, id)) {
            String key = object.getKey();
            ObjectMetadata objectMetadata = new ObjectMetadata(object.getObjectMetadata().getRawMetadata());

            ByteArrayPayload content = null;

            if (getContent) {
                S3ObjectInputStream contentStream = object.getObjectContent();
                if (verify) {
                    content = readAndVerifyContent(contentStream, BinaryUtils.fromHex(objectMetadata.getETag()));
                } else {
                    content = new ByteArrayPayload(IOUtils.toByteArray(contentStream));
                }
            } else {
                content = new ByteArrayPayload(new byte[0]);
            }
            content.close();

            result = new StorageObject(key,
                    getUri(key),
                    objectMetadata.getLastModified(),
                    objectMetadata,
                    content);
        } catch (IOException e){
            LOG.error("Error reading object content", e);
        }

        return result;
    }

    /**
     * Add a messageDigest to the provided stream and calculate the hash when reading is done.
     * Then the hash is compared to the hash on the server.
     * @param contentStream
     * @param serverSideHash
     * @return read and verified content from the stream
     * @throws ContentValidationException, IOException
     */
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
     * @return 
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

    public void deleteBucket(String bucket) {
        client.deleteBucket(bucket);
    }

    public void setS3ClientOptions(S3ClientOptions s3ClientOptions) {
        client.setS3ClientOptions(s3ClientOptions);
    }
}
