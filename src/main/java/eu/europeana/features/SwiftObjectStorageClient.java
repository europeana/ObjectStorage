package eu.europeana.features;


import eu.europeana.domain.ContentValidationException;
import eu.europeana.domain.ObjectMetadata;
import eu.europeana.domain.ObjectStorageClientException;
import eu.europeana.domain.StorageObject;
import org.jclouds.ContextBuilder;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * Created by jeroen on 14-12-16.
 */
public class SwiftObjectStorageClient implements ObjectStorageClient {

    private static final Logger LOG = LoggerFactory.getLogger(SwiftObjectStorageClient.class);

    private ObjectApi objectApi;
    private String bucketName;

    /**
     * Create a new Swift client and setup connection
     * @param authUrl
     * @param userName
     * @param password
     * @param containerName
     * @param regionName
     * @param tenantName
     */
    public SwiftObjectStorageClient(String authUrl, String userName, String password, String containerName, String regionName, String tenantName) {
        final SwiftApi swiftApi = ContextBuilder.newBuilder("openstack-swift")
                .credentials(tenantName + ":" + userName, password)
                .endpoint(authUrl)
                .buildApi(SwiftApi.class);

        final ContainerApi containerApi = swiftApi.getContainerApi(regionName);

        if (containerApi.get(containerName) == null && !containerApi.create(containerName)) {
                throw new ObjectStorageClientException("Swift cannot create container: " + containerName);
        }

        objectApi = swiftApi.getObjectApi(regionName, containerName);
        this.bucketName = containerName;
    }

    /**
     * @see ObjectStorageClient#getName()
     */
    @Override
    public String getName() {
        return "Swift";
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
        List<SwiftObject> results = objectApi.list();
        ArrayList<StorageObject> storageObjects = new ArrayList<>();
        for (SwiftObject so : results) {
            storageObjects.add(toStorageObject(so).get());
        }
        return storageObjects;
    }

    /**
     * @see ObjectStorageClient#put(String, Payload)
     */
    @Override
    public String put(String key, Payload value) {
        return objectApi.put(key, value);
    }

    private Optional<StorageObject> toStorageObject(SwiftObject so) {
        ObjectMetadata metadata = new ObjectMetadata();
        so.getMetadata().entrySet().forEach(entry -> metadata.addMetaData(entry.getKey(), entry.getValue()));
        return Optional.of(new StorageObject(so.getName(), so.getUri(), so.getLastModified(), metadata, so.getPayload()));
    }

    /**
     * @see ObjectStorageClient#put(StorageObject)
     */
    @Override
    public String put(StorageObject storageObject) {
        return objectApi.put(storageObject.getName(), storageObject.getPayload());
    }

    /**
     * @see ObjectStorageClient#getWithoutBody(String)
     */
    @Override
    public Optional<StorageObject> getWithoutBody(String objectName) {
        return toStorageObject(objectApi.getWithoutBody(objectName));
    }

    /**
     * @see ObjectStorageClient#get(String)
     */
    @Override
    public Optional<StorageObject> get(String objectName) {
        return toStorageObject(objectApi.get(objectName));
    }

    /**
     * @see ObjectStorageClient#get(String, boolean)
     */
    @Override
    public Optional<StorageObject> get(String objectName, boolean verify) throws ContentValidationException {
        if (verify) {
            throw new IllegalStateException("Verification not implemented yet");
        }
        return toStorageObject(objectApi.get(objectName));
    }

    /**
     * @see ObjectStorageClient#getContent(String)
     */
    @Override
    public byte[] getContent(String objectName) {
        return ((ByteArrayPayload) objectApi.get(objectName).getPayload()).getRawContent();
    }

    /**
     * @see ObjectStorageClient#delete(String)
     */
    @Override
    public void delete(String objectName) {
        objectApi.delete(objectName);
    }

    /**
     * @see ObjectStorageClient#close()
     */
    @Override
    public void close() {
        try {
            LOG.info("Shutting down connections to {} ...", this.getName());
            ((SwiftApi) objectApi).close();
        } catch (IOException e) {
            LOG.error("Error closing Swift client", e);
        }

    }
}
