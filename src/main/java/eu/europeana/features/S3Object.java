package eu.europeana.features;

import java.io.InputStream;
import java.util.Map;

/**
 * Wrapper around an input stream containing the S3 Object's data and a map with the object's metadata
 * Note that there is also a software.amazon.awssdk.services.s3.model.S3Object but that is only used as a summary then
 * listing objects in a bucket.
 */
public record S3Object(InputStream inputStream, Map<String, String> metadata) {

    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String LAST_MODIFIED = "Last-Modified";
    public static final String ETAG = "ETag";
    public static final String VERSION_ID = "VersionId";

}
