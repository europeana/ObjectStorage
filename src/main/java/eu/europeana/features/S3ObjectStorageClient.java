package eu.europeana.features;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.Md5Utils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * Client for accessing objects stored on Amazon S3 service.
 * Created by jeroen on 14-12-16.
 */
public class S3ObjectStorageClient implements ObjectStorageClient {

    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectStorageClient.class);

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
        try (InputStream is = value.openStream()){
            content = IOUtils.toByteArray(is);
        } catch (IOException e) {
            LOG.error("Error reading payload for key "+key, e);
        }
        Integer intLength = content.length;
        metadata.setContentLength(intLength.longValue());
        byte[] md5 = Md5Utils.computeMD5Hash(content);
        String md5Base64 = BinaryUtils.toBase64(md5);
        metadata.setContentMD5(md5Base64);

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
            return Optional.of(retrieveAsStorageObject(objectName, Boolean.FALSE));
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw new ObjectStorageClientException("Error while retrieving storageObject without body", ex);
            }
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#get(String)
     */
    @Override
    public Optional<StorageObject> get(String objectName) {
        try {
            return Optional.of(retrieveAsStorageObject(objectName, Boolean.TRUE));
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw ex;
            }
        }
    }

    /**
     * @see eu.europeana.features.ObjectStorageClient#getContentAsBytes(String)
     */
    @Override
    public Optional<byte[]> getContentAsBytes(String objectName) {
        try {
            return Optional.of(retrieveAsBytes(objectName));
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw new ObjectStorageClientException("Error while retrieving storage object", ex);
            }
        }
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
    private StorageObject retrieveAsStorageObject(String id, boolean getContent) {
        StorageObject result = null;
        try (S3Object object = client.getObject(bucketName, id)) {
            String key = object.getKey();
            ObjectMetadata objectMetadata = new ObjectMetadata(object.getObjectMetadata().getRawMetadata());
            ByteArrayPayload content = (getContent ? new ByteArrayPayload(IOUtils.toByteArray(object.getObjectContent())) : new ByteArrayPayload(new byte[0]));
            content.close();

            result = new StorageObject(key,
                    getUri(key),
                    objectMetadata.getLastModified(),
                    objectMetadata,
                    content);
        } catch (IOException e) {
            LOG.error("Error retrieving storage object with id "+id, e);
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
            LOG.error("Error retrieving storage object with id "+id, e);
        }
        return new byte[0];
    }

    /**
     *  @see ObjectStorageClient#verify(StorageObject)
     */
    @Override
    public boolean verify(StorageObject object) throws IOException {
        byte[] clientSideHash = null;
        byte[] serverSideHash = null;
        try (InputStream in = new ByteArrayInputStream(IOUtils.toByteArray(object.getPayload().openStream()))) {
            clientSideHash = Md5Utils.computeMD5Hash(in);
            serverSideHash = BinaryUtils.fromHex(object.getMetadata().getETag());
        }

        return (clientSideHash != null && serverSideHash != null
                && Arrays.equals(clientSideHash, serverSideHash));
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
