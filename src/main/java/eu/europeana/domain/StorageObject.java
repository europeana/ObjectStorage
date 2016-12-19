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
package eu.europeana.domain;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import eu.europeana.features.ObjectStorageClient;
import org.jclouds.io.Payload;

import java.net.URI;
import java.util.Date;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an object in Generic StorageObject
 * Modelled after org/apache/jclouds/api/openstack-swift/1.9.0/openstack-swift-1.9.0.jar!/org/jclouds/openstack/swift/v1/domain/SwiftObject.class
 * (Stripped down version)
 *
 * @see StorageObject
 */
public class StorageObject implements Comparable<StorageObject> {

    private final String name;
    private final URI uri;
    private final Payload payload;
    private ObjectMetadata metadata;

    public StorageObject(String name, URI uri, Date lastModified,
                         ObjectMetadata metadata, Payload payload) {
        this.name = checkNotNull(name, "name");
        this.uri = checkNotNull(uri, "uri of %s", uri);
        if (metadata == null) {
            metadata = new ObjectMetadata();
        }
        this.metadata = metadata;
        this.payload = payload;
    }

    /**
     * @return The name of this object.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The {@link URI} for this object.
     */
    public URI getUri() {
        return uri;
    }

    /**
     * @return The ETag of the content of this object.
     */
    public String getETag() {
        return metadata.getETag();
    }

    /**
     * @return The {@link Date} that this object was last modified.
     */
    public Date getLastModified() {
        return metadata.getLastModified();
    }

    /**
     * <h3>NOTE</h3>
     * The object will only have a {@link Payload#getInput()} when retrieved via the
     * {@link ObjectStorageClient#get(String) GetObject} command.
     *
     * @return The {@link Payload} for this object.
     */
    public Payload getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object instanceof StorageObject) {
            final StorageObject that = StorageObject.class.cast(object);
            return equal(getName(), that.getName())
                    && equal(getUri(), that.getUri())
                    && equal(getETag(), that.getETag());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName(), getUri(), getETag());
    }

    @Override
    public String toString() {
        return string().toString();
    }

    protected ToStringHelper string() {
        return toStringHelper(this)
                .add("name", getName())
                .add("uri", getUri())
                .add("etag", getETag())
                .add("lastModified", getLastModified());
    }

    //@Override
    public int compareTo(StorageObject that) {
        if (that == null)
            return 1;
        if (this == that)
            return 0;
        return this.getName().compareTo(that.getName());
    }

    public ObjectMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(ObjectMetadata metadata) {
        this.metadata = metadata;
    }
}
