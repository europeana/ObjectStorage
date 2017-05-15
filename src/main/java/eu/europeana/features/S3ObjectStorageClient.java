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
//TODO LOGGING

/**
 * Client for accessing objects stored on Amazon S3 service.
 * Created by jeroen on 14-12-16.
 */
public class S3ObjectStorageClient implements ObjectStorageClient {

    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectStorageClient.class);

    private AmazonS3 client;
    private String bucket;

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
        this.bucket = bucketName;
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
        this.bucket = bucketName;
    }

    /**
     * @see ObjectStorageClient#list()
     */
    @Override
    public List<StorageObject> list() {
        ObjectListing objectListing = client.listObjects(bucket.toString());
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
        String bucketLocation = client.getBucketLocation(bucket.toString());
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
        InputStream inputStream = null;
        try {
            inputStream = storageObject.getPayload().openStream();
            putObjectResult = client.putObject(new PutObjectRequest(bucket, storageObject.getName(), inputStream, checkMetaData(metadata)));
        } catch (IOException e) {
            LOG.error("Error storing object "+storageObject.getName(), e);
        } finally {
            closeQuietly(inputStream);
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
        InputStream is = null;
        try {
            is = value.openStream();
            content = IOUtils.toByteArray(is);
            is.close();
        } catch (IOException e) {
            LOG.error("Error storing payload for key "+key, e);
        } finally {
            closeQuietly(is);
        }
        Integer intLength = content.length;
        metadata.setContentLength(intLength.longValue());
        byte[] md5 = Md5Utils.computeMD5Hash(content);
        String md5Base64 = BinaryUtils.toBase64(md5);
        metadata.setContentMD5(md5Base64);

        PutObjectResult putObjectResult = null;
        InputStream inputStream = null;
        try {
            inputStream = new ByteArrayInputStream(content);
            putObjectResult = client.putObject(new PutObjectRequest(bucket, key, inputStream, checkMetaData(metadata)));
        } finally {
            closeQuietly(inputStream);
        }

        return (putObjectResult == null ? null : putObjectResult.getETag());
    }

    private com.amazonaws.services.s3.model.ObjectMetadata checkMetaData(com.amazonaws.services.s3.model.ObjectMetadata metadata) {
        if (metadata.getContentLength() == 0l) {
            throw new ObjectStorageClientException("The metadata ContentLength is manditory");
        }
        return metadata;
    }

    /**
     * @see ObjectStorageClient#getWithoutBody(String)
     */
    @Override
    public Optional<StorageObject> getWithoutBody(String objectName) {
        try {
            return Optional.of(retrieveStorageObject(objectName, Boolean.FALSE));
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
            return Optional.of(retrieveStorageObject(objectName, Boolean.TRUE));
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw ex;
            }
        }
    }

     /**
     * @see ObjectStorageClient#delete(String)
     */
    @Override
    public void delete(String objectName) {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket.toString(), objectName);
        client.deleteObject(deleteObjectRequest);
    }

    private StorageObject retrieveStorageObject(String id, boolean hasContent) {
        // 2017-05-12 Timing debug statements added for now as part of ticket #613.
        // Can be removed when it's confirmed that timing is improved
        long startTime = 0;
        //if (LOG.isDebugEnabled())
        { startTime = System.nanoTime(); }

        StorageObject result = null;

        S3Object object = client.getObject(bucket, id);

        long getObjectTime = 0;
        long verifyHashTime = 0;
        long createObjectTime = 0;
        //if (LOG.isDebugEnabled())
        { getObjectTime = System.nanoTime(); }

        if (object != null) {
            final byte[] content = getBytes(hasContent, object);
            ObjectMetadata objectMetadata = new ObjectMetadata(object.getObjectMetadata().getRawMetadata());
            if (hasContent) {
                byte[] clientSideHash = null;
                byte[] serverSideHash = null;
                InputStream inputStream = null;
                try {
                    inputStream = new ByteArrayInputStream(content);
                    clientSideHash = Md5Utils.computeMD5Hash(inputStream);
                    serverSideHash = BinaryUtils.fromHex(object.getObjectMetadata().getETag());
                    //if (LOG.isDebugEnabled()) {
                } catch (Exception e) {
                    LOG.warn("Unable to calculate MD5 hash to validate download: " + e.getMessage(), e);
                }
                finally {
                    closeQuietly(inputStream);
                }

                if (clientSideHash != null && serverSideHash != null
                        && !Arrays.equals(clientSideHash, serverSideHash)) {
                    throw new SecurityException("Unable to verify integrity of data download.  " +
                            "Client calculated content hash didn't match hash calculated by Amazon S3.  " +
                            "The data stored in '"
                            + "' may be corrupt.");
                }


                //if (LOG.isDebugEnabled()) {
                    verifyHashTime = System.nanoTime();
                    //output timing to get object and verify hash
                       //};

            }

            String key = object.getKey();
            try {
                object.close();
            } catch (IOException e) {
                LOG.error("Error closing object to releases any underlying system resources.", e);
            }
            result = new StorageObject(key,
                    getUri(key),
                    objectMetadata.getLastModified(),
                    objectMetadata,
                    new ByteArrayPayload(content)
            );
            createObjectTime = System.nanoTime();
            LOG.error("S3 retrieveStorageObject timing: "+(getObjectTime - startTime) / 1000+";"+(verifyHashTime - getObjectTime) / 1000+";"+(createObjectTime-verifyHashTime)/1000);
            return result;
        }
        return null;
    }

    private byte[] getBytes(Boolean withContent, S3Object object) {
        final byte[] content;
        try {
            content = withContent ? IOUtils.toByteArray(object.getObjectContent()) : new byte[0];
            object.close();
        } catch (IOException e) {
            throw new ObjectStorageClientException("Error while converting content to bytes", e);
        } finally {
            closeQuietly(object);
        }
        return content;
    }


    //
    public Bucket createBucket(String bucket) {
        return client.createBucket(bucket);
    }

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
