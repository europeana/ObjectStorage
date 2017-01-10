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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
//TODO LOGGING

/**
 * Created by jeroen on 14-12-16.
 */
public class S3ObjectStorageClient implements ObjectStorageClient {
    public static Logger logger = LoggerFactory.getLogger(S3ObjectStorageClient.class);
    private AmazonS3 client;
    private String bucket;

    public S3ObjectStorageClient(String clientKey, String secretKey, String region, String bucket) {
        AWSCredentials credentials = new BasicAWSCredentials(clientKey, secretKey);
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region).build();
        this.bucket = bucket;
    }

    public S3ObjectStorageClient(String clientKey, String secretKey, String bucketName, String endpoint, S3ClientOptions s3ClientOptions) {
        client = new AmazonS3Client(new BasicAWSCredentials(clientKey, secretKey));
        client.setS3ClientOptions(s3ClientOptions);
        client.setEndpoint(endpoint);
        this.bucket = bucketName;
    }


    public List<StorageObject> list() {
        ObjectListing objectListing = client.listObjects(bucket.toString());
        List<S3ObjectSummary> results = objectListing.getObjectSummaries();
        ArrayList<StorageObject> storageObjects = new ArrayList<StorageObject>();
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
     * @param storageObject
     * @return ETag
     */
    public String put(StorageObject storageObject) {
        com.amazonaws.services.s3.model.ObjectMetadata metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
        metadata.setContentType(storageObject.getMetadata().getContentType());
        metadata.setContentLength(storageObject.getMetadata().getContentLength());
        metadata.setContentMD5(storageObject.getMetadata().getContentMD5());
        PutObjectResult putObjectResult = client.putObject(new PutObjectRequest(bucket, storageObject.getName(), storageObject.getPayload().getInput(), checkMetaData(metadata)));
        return putObjectResult.getETag();
    }

    public String put(String key, Payload value) {
        com.amazonaws.services.s3.model.ObjectMetadata metadata = new com.amazonaws.services.s3.model.ObjectMetadata();

        byte[] content = new byte[0];
        try {
            content = IOUtils.toByteArray(value.getInput());
        } catch (IOException e) {
            e.printStackTrace();
        }
        Integer intLength = new Integer(content.length);
        metadata.setContentLength(intLength.longValue());
        byte[] md5 = Md5Utils.computeMD5Hash(content);
        String md5Base64 = BinaryUtils.toBase64(md5);

        metadata.setContentMD5(md5Base64);

        PutObjectResult putObjectResult = client.putObject(new PutObjectRequest(bucket, key, value.getInput(), checkMetaData(metadata)));
        return putObjectResult.getETag();
    }

    private com.amazonaws.services.s3.model.ObjectMetadata checkMetaData(com.amazonaws.services.s3.model.ObjectMetadata metadata) {
        if (metadata.getContentLength() == 0l) {
            throw new ObjectStorageClientException("The metadata ContentLength is manditory");
        }
        return metadata;
    }

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


    public void delete(String objectName) {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket.toString(), objectName);
        client.deleteObject(deleteObjectRequest);
    }


    private StorageObject retrieveStorageObject(String id, boolean hasContent) {
        S3Object object = client.getObject(bucket, id);
        if (object != null) {
            final byte[] content = getBytes(hasContent, object);
            ObjectMetadata objectMetadata = new ObjectMetadata(object.getObjectMetadata().getRawMetadata());
            if (hasContent) {
                byte[] clientSideHash = null;
                byte[] serverSideHash = null;
                try {
                    clientSideHash = Md5Utils.computeMD5Hash(new ByteArrayInputStream(content));
                    serverSideHash = BinaryUtils.fromHex(object.getObjectMetadata().getETag());
                } catch (Exception e) {
                    logger.warn("Unable to calculate MD5 hash to validate download: " + e.getMessage(), e);
                }

                if (clientSideHash != null && serverSideHash != null
                        && !Arrays.equals(clientSideHash, serverSideHash)) {
                    throw new SecurityException("Unable to verify integrity of data download.  " +
                            "Client calculated content hash didn't match hash calculated by Amazon S3.  " +
                            "The data stored in '"
                            + "' may be corrupt.");
                }
            }

            String key = object.getKey();
            try {
                object.close();
            } catch (IOException e) {
                logger.error("Error closing object to releases any underlying system resources.", e);
            }
            return new StorageObject(key,
                    getUri(key),
                    objectMetadata.getLastModified(),
                    objectMetadata,
                    new ByteArrayPayload(content)

            );
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
            org.apache.commons.io.IOUtils.closeQuietly(object);
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
