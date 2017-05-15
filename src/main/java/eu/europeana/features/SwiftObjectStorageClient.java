package eu.europeana.features;


import eu.europeana.domain.ObjectMetadata;
import eu.europeana.domain.ObjectStorageClientException;
import eu.europeana.domain.StorageObject;
import org.jclouds.ContextBuilder;
import org.jclouds.io.Payload;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * Created by jeroen on 14-12-16.
 */
public class SwiftObjectStorageClient implements ObjectStorageClient {
    private ObjectApi objectApi;

    public SwiftObjectStorageClient(String authUrl, String userName, String password, String containerName, String regionName, String tenantName) {
        final SwiftApi swiftApi = ContextBuilder.newBuilder("openstack-swift")
                .credentials(tenantName + ":" + userName, password)
                .endpoint(authUrl)
                .buildApi(SwiftApi.class);

        final ContainerApi containerApi = swiftApi.getContainerApi(regionName);

        if (containerApi.get(containerName) == null) {
            if (!containerApi.create(containerName)) {
                throw new ObjectStorageClientException("Swift cannot create container: " + containerName);
            }
        }

        objectApi = swiftApi.getObjectApi(regionName, containerName);
    }

    public List<StorageObject> list() {
        List<SwiftObject> results = objectApi.list();
        ArrayList<StorageObject> storageObjects = new ArrayList<StorageObject>();
        for (SwiftObject so : results) {
            storageObjects.add(toStorageObject(so).get());
        }
        return storageObjects;
    }

    @Override
    public String put(String key, Payload value) {
        return objectApi.put(key, value);
    }

    private Optional<StorageObject> toStorageObject(SwiftObject so) {
        ObjectMetadata metadata = new ObjectMetadata();
        so.getMetadata().entrySet().forEach(entry -> {
            metadata.addMetaData(entry.getKey(), entry.getValue());
        });
        return Optional.of(new StorageObject(so.getName(), so.getUri(), so.getLastModified(), metadata, so.getPayload()));
    }

    public String put(StorageObject storageObject) {
        return objectApi.put(storageObject.getName(), storageObject.getPayload());
    }

    public Optional<StorageObject> getWithoutBody(String objectName) {
        return toStorageObject(objectApi.getWithoutBody(objectName));
    }

    public Optional<StorageObject> get(String objectName) {
        return toStorageObject(objectApi.get(objectName));
    }

    @Override
    public Optional<byte[]> getContentAsBytes(String objectName) {
        throw new IllegalStateException("Not implemented yet");
    }

    @Override
    public boolean verify(StorageObject object) throws IOException {
        throw new IllegalStateException("Not implemented yet");
    }


    public void delete(String objectName) {
        objectApi.delete(objectName);
    }
}
