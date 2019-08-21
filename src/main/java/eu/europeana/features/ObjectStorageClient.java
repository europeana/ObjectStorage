/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.europeana.features;


import eu.europeana.domain.ContentValidationException;
import eu.europeana.domain.ObjectMetadata;
import eu.europeana.domain.StorageObject;
import org.jclouds.io.Payload;

import java.util.List;
import java.util.Optional;

/**
 *  Interface to a generic object storage client
 */
public interface ObjectStorageClient {

    /**
     * @return the name of this object storage provider
     */
    String getName();

    /**
     *
     * @return the name of the bucket
     */
    String getBucketName();

    /**
     * @return an {@link List<StorageObject>}.
     */
    List<StorageObject> list();

    /**
     * Check if an object with the provided id exists
     * @param id
     * @return true if it exists, false if it does not exist
     */
    boolean isAvailable(String id);

    /**
     * Creates or updates a {@link StorageObject}.
     *
     * @param id  corresponds to {@link StorageObject#getName()}.
     * @param value corresponds to {@link StorageObject#getPayload()}.
     * @return {@link StorageObject#getETag()} of the object.
     */
    String put(String id, Payload value);

    /**
     * Creates or updates a {@link StorageObject}.
     *
     * @param storageObject corresponds to {@link StorageObject#getName()}.
     * @return {@link StorageObject#getETag()} of the object.
     */
    String put(StorageObject storageObject);

    /**
     * Gets the {@link StorageObject} metadata without its {@link Payload#openStream() body}.
     *
     * @param objectName corresponds to {@link StorageObject#getName()}.
     * @return the {@link StorageObject} or empty {@code Optional}, if not found.
     */
    Optional<StorageObject> getWithoutBody(String objectName);

    /**
     * Gets the {@link StorageObject} including its {@link Payload#openStream() body}.
     * Note that as of version 1.3 we no longer verify if the object was retrieved correctly. To do this you need to use
     * {@link ObjectStorageClient#get(String, boolean)} method
     *
     * @param objectName corresponds to {@link StorageObject#getName()}.
     * @return the {@link StorageObject} or empty {@code Optional}, if not found.
     */
    Optional<StorageObject> get(String objectName);

    /**
     * Gets the {@link StorageObject} including its {@link Payload#openStream() body}.
     *
     * @param objectName corresponds to {@link StorageObject#getName()}.
     * @param verify if true then the MD5 hash of the downloaded content is compared to the known MD5 hash stored on the
     *               server
     * @return the {@link StorageObject} or empty {@code Optional}, if not found.
     * @throws ContentValidationException thrown when verification of the validity of downloaded content fails.
     */
    Optional<StorageObject> get(String objectName, boolean verify) throws ContentValidationException;

    /**
     * Get the content of the specified object and return it as a byte array. This is the recommended (fastest)
     * method of retrieving media content like images.
     * @param objectName corresponds to {@link StorageObject#getName()}.
     * @return byte array representing the retrieved object, or empty byte array if no object was retrieved
     */
    byte[] getContent(String objectName);

    /**
     * Get the metadata of the specified object
     * @param objectName corresponds to {@link StorageObject#getName()}.
     * @return ObjectMetadata of the requests object or null if not found.
     */
    ObjectMetadata getMetaData(String objectName);

    /**
     * Deletes an object, if present.
     *
     * @param objectName corresponds to {@link StorageObject#getName()}.
     */
    void delete(String objectName);

    /**
     * Terminate all connections and shut down this client
     */
    void close();
}
