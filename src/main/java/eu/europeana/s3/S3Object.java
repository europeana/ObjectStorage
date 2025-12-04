package eu.europeana.s3;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;

/**
 * Wrapper around an input stream containing the S3 Object's data and a map with the object's metadata
 * Note that there is also a software.amazon.awssdk.services.s3.model.S3Object but that is only used as a summary then
 * listing objects in a bucket.
 */
public record S3Object(String key, InputStream inputStream, Map<String, Object> metadata) implements AutoCloseable {

    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String LAST_MODIFIED = "Last-Modified";
    public static final String ETAG = "ETag";
    public static final String VERSION_ID = "VersionId";

    public Long getContentLength() {
        if (metadata == null || metadata.get(CONTENT_LENGTH) == null) {
            return null;
        }
        if (metadata.get(CONTENT_LENGTH) instanceof Long contentLength) {
            return contentLength;
        }
        if (metadata.get(CONTENT_LENGTH) instanceof Integer contentLength) {
            return (long) contentLength;
        }
        return Long.valueOf(metadata.get(CONTENT_LENGTH).toString());
    }

    public String getContentType() {
        if (metadata == null  || metadata.get(CONTENT_TYPE) == null) {
            return null;
        }
        return metadata.get(CONTENT_TYPE).toString();
    }

    public String getContentEncoding() {
        if (metadata == null || metadata.get(CONTENT_ENCODING) == null) {
            return null;
        }
        return metadata.get(CONTENT_ENCODING).toString();
    }

    public Instant getLastModified() {
        if (metadata == null || metadata.get(LAST_MODIFIED) == null) {
            return null;
        }
        if (metadata.get(LAST_MODIFIED) instanceof Instant lastModified) {
            return lastModified;
        }
        return Instant.parse(String.valueOf(metadata.get(LAST_MODIFIED)));
    }

    public String getETag() {
        if (metadata == null || metadata.get(ETAG) == null) {
            return null;
        }
        return metadata.get(ETAG).toString();
    }

    public String getVersionId() {
        if (metadata == null || metadata.get(VERSION_ID) == null) {
            return null;
        }
        return metadata.get(VERSION_ID).toString();
    }

    @Override
    public void close() throws IOException {
        if (this.inputStream != null) {
            this.inputStream.close();
        }
    }
}
