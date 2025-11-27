package eu.europeana.features;

import eu.europeana.exception.S3ObjectStorageException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for testing (the speed of connections to) Amazon S3 and IBM S3 storage
 * For this test to work properly you need to place an objectstorage.IT.properties in the src/main/test/resources folder
 * This file needs to contain the following keys that point to an existing bucket at S3 (s3.key, s3.secret, s3.region, s3.bucket).
 * Created by Jeroen Jeurissen on 18-12-16
 * Major refactoring by Patrick Ehlert on 8 Feb 2017 and revised Jun 2024 and Nov 2025
 */
@SuppressWarnings("java:S5786")
public class S3ObjectStorageClientIT {

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClientIT.class);

    private static final int NANO_TO_MS = 1000_000;

    // Run tests in IBM S3 (not Amazon S3)
    private static final boolean IBM_S3_TEST = true;

    private static final String TEST_TEXT_OBJECT_ID = "test-object";
    private static final String TEST_TEXT_OBJECT_DATA = "This is just some text to test storing data in S3...";

    private static final String TEST_IMAGE_FILENAME = "logo.png";

    private static final String TEST_OBJECT_NOT_EXISTS = "NonExistingObjectId";

    private static S3ObjectStorageClient client;

    // We time the various retrieval methods to see which is fastest
    // For this we use the properties below
    private static long nrItems = 0;
    private static long timingMetadata = 0;
    private static long timingObjectStream = 0;
    private static long timingObjectStreamRead = 0;
    private static long timingObjectAndMetadata = 0;
    private static long timingObjectBytes = 0;


    @BeforeAll
    public static void initClientAndConnectToStorage() throws IOException, URISyntaxException {
        Properties prop = loadAndCheckLoginProperties();
        if (IBM_S3_TEST) {
            client = new S3ObjectStorageClient(prop.getProperty("s3.key")
                    , prop.getProperty("s3.secret")
                    , prop.getProperty("s3.region")
                    , prop.getProperty("s3.bucket")
                    , new URI(prop.getProperty("s3.endpoint")));
        } else {
            client = new S3ObjectStorageClient(prop.getProperty("s3.key")
                    , prop.getProperty("s3.secret")
                    , prop.getProperty("s3.region")
                    , prop.getProperty("s3.bucket"));
        }
    }

    private static Properties loadAndCheckLoginProperties() throws IOException {
        Properties prop = new Properties();
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("objectstorage.IT.properties")) {
            if (in == null) {
                throw new RuntimeException("Please provide objectstorage.IT.properties file with login details");
            }
            prop.load(in);
            // check if the properties contain login details for test and not production
            String bucketName = prop.getProperty("s3.bucket");
            if (bucketName != null && bucketName.contains("production")) {
                throw new RuntimeException("Do not use production settings for unit tests!");
            }
        }
        return prop;
    }

    @Test
    public void testGetServiceName() {
        if (IBM_S3_TEST) {
            assertTrue(client.getServiceName().contains("IBM"));
        } else {
            assertTrue(client.getServiceName().contains("Amazon"));
        }
    }

    @Test
    public void testListBuckets() {
        LOG.info("Available buckets are:");
        for (Bucket bucket : client.listBuckets()) {
            assertNotNull(bucket);
            LOG.info("  {}", bucket.name());
        }

    }

    @Test
    public void testGetBucket() {
        String bucketName = client.getBucketName();
        LOG.info("Currently used bucket is {}", bucketName);
        assertNotNull(bucketName);
    }

    @Test
    public void testListFirst50() {
        int nrResults = 50;
        ListObjectsV2Response list = client.listAll(null, nrResults);
        assertEquals(nrResults, list.keyCount());
    }

    /**
     * Note that this will only work if the used bucket doesn't contain lots of objects, otherwise this test may raise
     * an OOM
     */
    @Test
    public void testListAllObjectsWithToken() {
        String continuationToken = null;
        long count = 0;
        do {
            ListObjectsV2Response list = client.listAll(continuationToken);
            continuationToken = list.nextContinuationToken();
            count = count + list.keyCount();
            for (software.amazon.awssdk.services.s3.model.S3Object obj : list.contents()) {
                LOG.trace("  {}, {}, {}, {}", obj.key(), obj.eTag(), obj.lastModified(), obj.size());
                assertNotNull(obj.key());
            }
            LOG.info("  Retrieved {} keys...", count);
        } while (continuationToken != null);
        LOG.info("Found {} storage objects in bucket {}...", count, client.getBucketName());
        assertTrue(count > 100);
    }

    @Test
    public void testIsObjectAvailable() {
        String objectId = TEST_TEXT_OBJECT_ID;
        deleteOldTestObject(objectId);
        assertFalse(client.isObjectAvailable(objectId));

        byte[] data = TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        String contentType = "text/plain";
        String eTag = client.putObject(objectId, contentType, data);
        assertTrue(client.isObjectAvailable(objectId));
        assertNotNull(eTag);

        client.deleteObject(objectId);
        assertFalse(client.isObjectAvailable(objectId));
    }

    @Test
    public void testPutObjectAsBytes() {
        String objectId = TEST_TEXT_OBJECT_ID;
        byte[] data = TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        String contentType = "text/plain";
        String eTag = client.putObject(objectId, contentType, data);
        assertNotNull(eTag);

        // verify it's available and stored with the correct (meta)data
        Map<String, Object> metadata = client.getObjectMetadata(objectId);
        assertNotNull(metadata);
        assertEquals(eTag, metadata.get(S3Object.ETAG));
        assertEquals(contentType, metadata.get(S3Object.CONTENT_TYPE));
        assertEquals((long) data.length, metadata.get(S3Object.CONTENT_LENGTH));
        assertNotNull(metadata.get(S3Object.LAST_MODIFIED));

        client.deleteObject(objectId);
        assertFalse(client.isObjectAvailable(objectId));
    }

    @Test
    public void testPutObjectAsStream()  throws IOException  {
        String objectId= TEST_IMAGE_FILENAME;
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(objectId)) {
            assertNotNull(in);
            String contentType = "image/webp";
            Long length = (long) in.available();
            String eTag = client.putObject(objectId, contentType, in);
            assertNotNull(eTag);

            // verify it's available and stored with the correct (meta)data
            Map<String, Object> metadata = client.getObjectMetadata(objectId);
            assertNotNull(metadata);
            assertEquals(eTag, metadata.get(S3Object.ETAG));
            assertEquals(contentType, metadata.get(S3Object.CONTENT_TYPE));
            assertEquals(length, metadata.get(S3Object.CONTENT_LENGTH));
            // once stored last modified date will be automatically set by S3
            assertNotNull(metadata.get(S3Object.LAST_MODIFIED));

            client.deleteObject(objectId);
            assertFalse(client.isObjectAvailable(objectId));
        }
    }

    @Test
    public void testPutObjectAndGetObjectMetadata() {
        String objectId = TEST_TEXT_OBJECT_ID;
        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = null;

        byte[] data = TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        String contentType = "text/csv";
        Map<String, String> metadataIn = new HashMap<>();
        metadataIn.put(key1, value1);
        metadataIn.put(key2, value2);
        String eTag = client.putObject(objectId, contentType, new ByteArrayInputStream(data), metadataIn);
        assertNotNull(eTag);

        // verify object is available and stored with the expected metadata
        Map<String, Object> metadataOut = client.getObjectMetadata(objectId);
        assertNotNull(metadataOut);
        assertEquals(eTag, metadataOut.get(S3Object.ETAG));
        assertEquals(contentType, metadataOut.get(S3Object.CONTENT_TYPE));
        assertTrue(metadataOut.containsKey(key1));
        assertEquals(value1, metadataOut.get(key1));
        // apparently S3 only stores keys if there is a value!
        assertFalse(metadataOut.containsKey(key2));

        client.deleteObject(objectId);
        assertFalse(client.isObjectAvailable(objectId));
    }

    @Test
    public void testPutBytesNoContentType() {
        byte[] data = TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        assertThrows(S3ObjectStorageException.class, () -> client.putObject(TEST_TEXT_OBJECT_ID, null, data));
    }

    @Test
    public void testPutStreamNoContentType() {
        ByteArrayInputStream stream = new ByteArrayInputStream(TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8));
        assertThrows(S3ObjectStorageException.class, () -> client.putObject(TEST_TEXT_OBJECT_ID, null, stream));
    }

    @Test
    public void testGetObjectBytes() {
        String objectId = TEST_TEXT_OBJECT_ID;
        byte[] data = TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        String contentType = "text/plain";
        String eTag = client.putObject(objectId, contentType, new ByteArrayInputStream(data));
        assertNotNull(eTag);

        byte[] retrieved = client.getObjectAsBytes(objectId);
        assertEquals(TEST_TEXT_OBJECT_DATA, new String(retrieved));

        client.deleteObject(objectId);
    }

    @Test
    public void testGetObjectStream() throws IOException {
        String objectId= TEST_IMAGE_FILENAME;
        Integer length;
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(objectId)) {
            assertNotNull(in);
            length = in.available();
            String contentType = "image/webp";
            String eTag = client.putObject(objectId, contentType, in);
            assertNotNull(eTag);
        }

        InputStream stream = client.getObjectAsStream(objectId);
        assertNotNull(stream);
        byte[] byteArray = IOUtils.toByteArray(stream);
        assertEquals(length, byteArray.length);
    }

    @Test
    public void testGetObjectAndMetadata()  {
        String objectId = TEST_TEXT_OBJECT_ID;
        byte[] data = TEST_TEXT_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        String contentType = "text/plain";
        Map<String, String> metadataIn = new HashMap<>();
        String key1 = "key1";
        String value1 = "value1";
        metadataIn.put(key1, value1);
        String eTag = client.putObject(objectId, contentType, new ByteArrayInputStream(data), metadataIn);
        assertNotNull(eTag);

        eu.europeana.features.S3Object result = client.getObject(objectId);
        assertNotNull(result);
        assertNotNull(result.inputStream());
        assertNotNull(result.metadata());
        assertEquals(eTag, result.getETag());
        assertEquals(contentType, result.getContentType());
        assertTrue(result.metadata().containsKey(key1));
        assertEquals(value1, result.metadata().get(key1));
    }


    /**
     * Test what happens if the metadata of an object does not exist
     */
    @Test
    public void testGetMetaDataNotExist() {
        assertNull(client.getObjectMetadata(TEST_OBJECT_NOT_EXISTS));
    }

    /**
     * Test what happens if an object does not exist
     */
    @Test
    public void testGetObjectNotExist() throws IOException {
        String objectId = TEST_OBJECT_NOT_EXISTS;
        // retrieve as bytes
        byte[] result = client.getObjectAsBytes(objectId);
        assertEquals(0, result.length);

        // retrieve as stream
        try (InputStream s = client.getObjectAsStream(objectId)) {
            assertNull(s);
        }

        // retrieve stream and metadata
        S3Object s3object = client.getObject(objectId);
        assertNull(s3object);
    }


    /**
     * Does a stress test of putting, retrieving in different ways and deleting a small test object.
     * Note that this may take a while (approx. 3.5 minutes for 1000 items, or 23 sec for 100 items)
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void testPerformance() throws IOException {
        String objectId =  TEST_IMAGE_FILENAME;
        deleteOldTestObject(objectId);

        final int TEST_SIZE = 100;
        LOG.info("Starting performance test with size {}...", TEST_SIZE);

        for (int i = 0; i < TEST_SIZE ; i++) {
            try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(objectId)) {
                assertNotNull(in);
                String contentType = "image/webp";
                String eTag = client.putObject(objectId, contentType, in);
                assertNotNull(eTag);
            }
            testRetrieval(objectId);
            client.deleteObject(objectId);
            // provide feedback about progress
            if (i % 50 == 0) {
                LOG.info("{}%", String.format("%.1f", i * 100.0 / TEST_SIZE) );
            }
        }

        LOG.info("Performance test done.");
    }

    /**
     * Check if our default test object already exists, if so it's probably from a previous test so we delete it
     */
    private void deleteOldTestObject(String id) {
        if (client.isObjectAvailable(id)) {
            LOG.warn("Found previous test object, deleting it.");
            client.deleteObject(id);
        }
    }

    /**
     * We support different methods to retrieve data, in this method we test all. The differences in speed are usually
     * small and depend largely on network latency and S3 service performance. Generally, getObjectMetadata is fastest,
     * then getObjectStream without reading.
     * @param id object to use for retrieval tests
     */
    private void testRetrieval(String id) throws IOException {
        long start;
        byte[] content;

        // 1. Retrieve only metadata not object itself, fastest of all 4 methods but doesn't get actual content
        start = System.nanoTime();
        Map<String, Object> metadata = client.getObjectMetadata(id);
        timingMetadata += (System.nanoTime() - start);
        assertNotNull(metadata);

        // 2. Retrieve content as stream and read it (similar performance as get object full + read)
        start = System.nanoTime();
        try (InputStream is = client.getObjectAsStream(id)) {
            timingObjectStream += (System.nanoTime() - start);
            content = IOUtils.toByteArray(is);
            timingObjectStreamRead += (System.nanoTime() - start);
            assertNotNull(content);
        }

        // 3. Retrieve as full object (so inputstream and metadata map, we read the stream for fair comparison)
        start = System.nanoTime();
        S3Object s3Object = client.getObject(id);
        content = IOUtils.toByteArray(s3Object.inputStream());
        timingObjectAndMetadata += (System.nanoTime() - start);
        assertNotNull(content);
        assertNotNull(s3Object.metadata());

        // 4. Retrieve content as byte array
        start = System.nanoTime();
        content = client.getObjectAsBytes(id);
        timingObjectBytes += (System.nanoTime() - start);
        assertNotNull(content);

        nrItems++;
    }

    @AfterAll
    public static void printTimings() {
        client.close();

        LOG.info("Time spend on retrieval of {} items (in ms)...", nrItems);
        LOG.info("  GetObject metadata only  : {} ", timingMetadata / NANO_TO_MS);
        LOG.info("  GetObject as stream      : {}", timingObjectStream / NANO_TO_MS);
        LOG.info("  GetObject as stream+read : {}", timingObjectStreamRead / NANO_TO_MS);
        LOG.info("  GetObject full+read      : {} ", timingObjectAndMetadata / NANO_TO_MS);
        LOG.info("  GetObject as byte[]      : {} ", timingObjectBytes / NANO_TO_MS);
    }

}
