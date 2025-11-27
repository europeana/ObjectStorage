package eu.europeana.features;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class S3ObjectTest {

    @Test
    public void S3ObjectInputStream() {
        InputStream test = new ByteArrayInputStream("test".getBytes(Charset.defaultCharset()));
        S3Object s3Object = new S3Object(test, null);

        assertEquals(test, s3Object.inputStream());
        assertNull(s3Object.metadata());
    }

    @Test
    public void S3ObjectMetadata() {
        Long contentLength = 100L;
        String contentType = "application/json";
        String contentEncoding = "UTF-8";
        Instant lastModified = Instant.parse("2025-11-25T09:08:00Z");
        String eTag = "123456789abcde";
        String versionId = "v1.2";
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(S3Object.CONTENT_LENGTH, contentLength);
        metadata.put(S3Object.CONTENT_TYPE, contentType);
        metadata.put(S3Object.CONTENT_ENCODING, contentEncoding);
        metadata.put(S3Object.LAST_MODIFIED, lastModified);
        metadata.put(S3Object.ETAG, eTag);
        metadata.put(S3Object.VERSION_ID, versionId);
        S3Object s3Object = new S3Object(null, metadata);

        assertNull(s3Object.inputStream());
        assertEquals(contentLength, s3Object.getContentLength());
        assertEquals(contentType, s3Object.getContentType());
        assertEquals(contentEncoding, s3Object.getContentEncoding());
        assertEquals(lastModified, s3Object.getLastModified());
        assertEquals(eTag, s3Object.getETag());
        assertEquals(versionId, s3Object.getVersionId());
    }

    /**
     * Check if conversion of Long and Instant to string and back works fine
     */
    @Test
    public void S3ObjectMetadataFromString() {
        Long contentLength = 100L;
        Instant lastModified = Instant.parse("2025-11-25T09:08:00Z");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(S3Object.CONTENT_LENGTH, contentLength.toString());
        metadata.put(S3Object.LAST_MODIFIED, lastModified.toString());
        S3Object s3Object = new S3Object(null, metadata);

        assertEquals(contentLength, s3Object.getContentLength());
        assertEquals(lastModified, s3Object.getLastModified());
    }

    @Test
    public void S3ObjectMetadataNullValues() {
        S3Object s3Object = new S3Object(null, null);
        assertNull(s3Object.getContentLength());
        assertNull(s3Object.getContentType());
        assertNull(s3Object.getContentEncoding());
        assertNull(s3Object.getLastModified());
        assertNull(s3Object.getETag());
        assertNull(s3Object.getVersionId());
    }
}
