package eu.europeana.features;

import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for testing (the speed of connections to) Amazon S3 and IBM S3 storage
 * For this test to work properly you need to place an objectstorage.IT.properties in the src/main/test/resources folder
 * This file needs to contain the following keys that point to an existing bucket at S3 (s3.key, s3.secret, s3.region, s3.bucket).
 *
 * Created by Jeroen Jeurissen on 18-12-16
 * Updated by Patrick Ehlert on 8 Feb 2017 and revised Jun 2024
 */
public class S3ObjectStorageClientIT {

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClientIT.class);

    private static final int NANO_TO_MS = 1000_000;

    private static final boolean runBluemixTest = true;

    private static final String TEST_OBJECT_NAME = "test-object";
    private static final String TEST_OBJECT_DATA = "This is just some text to test storing data in S3...";
    private static final String TEST_OBJECT_DATA_MD5 = "Fl6sF5ph/K/FRkP55hJ3Rw==";

    private static final String TEST_OBJECT_NOT_EXISTS = "This is an id to an non-existing s3 object";

    private static S3ObjectStorageClient client;

    // We time the various retrieval methods to see which is fastest
    // For this we use the properties below
    private static long nrItems = 0;
    private static long timingS3Object = 0;
    private static long timingMetadata = 0;
    private static long timingContentStream = 0;
    private static long timingContentBytes = 0;

    @BeforeAll
    public static void initClientAndTestServer() throws IOException {
        Properties prop = loadAndCheckLoginProperties();
        if (runBluemixTest) {
            client = new S3ObjectStorageClient(prop.getProperty("s3.key")
                    , prop.getProperty("s3.secret")
                    , prop.getProperty("s3.region")
                    , prop.getProperty("s3.bucket")
                    , prop.getProperty("s3.endpoint"));
        } else {
            client = new S3ObjectStorageClient(prop.getProperty("s3.key")
                    , prop.getProperty("s3.secret")
                    , prop.getProperty("s3.region")
                    , prop.getProperty("s3.bucket"),
                    (Integer) null);
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
    public void testListBuckets() {
        LOG.info("Available buckets are:");
        for (Bucket bucket : client.listBuckets()) {
            assertNotNull(bucket);
            LOG.info("  {}", bucket.getName());
        }

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
            ListObjectsV2Result list = client.listAll(continuationToken, 100);
            continuationToken = list.getNextContinuationToken();
            count = count + list.getKeyCount();
            List<S3ObjectSummary> contents = list.getObjectSummaries();
            for (S3ObjectSummary summary : contents) {
                assertNotNull(summary.getKey());
            }
            LOG.info("  Retrieved {} keys...", count);
        } while (continuationToken != null);
        LOG.info("Found {} storage objects in bucket {}...", count, client.getBucketName());
        assertTrue(count > 100);
    }

    @Test
    public void testListFirst50() {
        int nrResults = 50;
        ListObjectsV2Result list = client.listAll(null, nrResults);
        assertEquals(nrResults, list.getKeyCount());
    }

    @Test
    public void testGenerateMetaData() {
        byte[] data = TEST_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = client.generateObjectMetadata(data);
        assertEquals(data.length, metadata.getContentLength());
        assertEquals(TEST_OBJECT_DATA_MD5, metadata.getContentMD5());

        ObjectMetadata metadata2 = client.generateObjectMetadata(TEST_OBJECT_NAME, new ByteArrayInputStream(data));
        assertEquals(metadata.getContentLength(), metadata2.getContentLength());
        assertEquals(metadata.getContentMD5(), metadata2.getContentMD5());
    }

    @Test
    public void testPutAndIsAvailable() {
        deleteOldTestObject(TEST_OBJECT_NAME);
        assertFalse(client.isObjectAvailable(TEST_OBJECT_NAME));

        byte[] data = TEST_OBJECT_DATA.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = client.generateObjectMetadata(data);
        metadata.setContentType("text/plain");
        metadata.setContentEncoding("UTF-8");
        metadata.setLastModified(new Date());
        String eTag = client.putObject(TEST_OBJECT_NAME, new ByteArrayInputStream(data), metadata);

        assertTrue(client.isObjectAvailable(TEST_OBJECT_NAME));
        assertNotNull(eTag);

        client.deleteObject(TEST_OBJECT_NAME);
        assertFalse(client.isObjectAvailable(TEST_OBJECT_NAME));
    }

    @Test
    public void testPutAndRetrieval() {
        deleteOldTestObject(TEST_OBJECT_NAME);

        String eTag = client.putObject(TEST_OBJECT_NAME, TEST_OBJECT_DATA);

        assertNotNull(eTag);
        byte[] retrieved = client.getObjectContent(TEST_OBJECT_NAME);
        assertEquals(TEST_OBJECT_DATA, new String(retrieved));

        client.deleteObject(TEST_OBJECT_NAME);
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
    public void testGetS3ObjectNotExist() throws IOException {
        try (S3Object s3Object = client.getObject(TEST_OBJECT_NOT_EXISTS)) {
            assertNull(s3Object);
        }
    }

    @Test
    public void testGetContentStreamNotExists() throws IOException {
        try (InputStream is = client.getObjectStream(TEST_OBJECT_NOT_EXISTS)) {
            assertNull(is);
        }
    }

    @Test
    public void testGetContentBytesNotExists() {
        assertEquals(0, client.getObjectContent(TEST_OBJECT_NOT_EXISTS).length);
    }

    /**
     * Does a stress test of putting, retrieving in different ways and deleting a small test object.
     * Note that this may take a while (approx. 3.5 minutes for 1000 items, or 23 sec for 100 items)
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void testPerformance() throws IOException {
        deleteOldTestObject(TEST_OBJECT_NAME);

        final int TEST_SIZE = 100;
        LOG.info("Starting performance test with size {}", TEST_SIZE);

        ObjectMetadata metadata = client.generateObjectMetadata(TEST_OBJECT_DATA.getBytes(StandardCharsets.UTF_8));
        metadata.setContentType("text/plain");
        metadata.setContentEncoding("UTF-8");
        metadata.setLastModified(new Date());

        for (int i = 0; i < TEST_SIZE ; i++) {
            client.putObject(TEST_OBJECT_NAME, IOUtils.toInputStream(TEST_OBJECT_DATA, StandardCharsets.UTF_8), metadata);
            testRetrieval(TEST_OBJECT_NAME);
            client.deleteObject(TEST_OBJECT_NAME);
            // provide feedback about progress
            if (i % 50 == 0) {
                LOG.info(String.format("%.1f", i * 100.0 / TEST_SIZE) +"%");
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
     * We support different methods to retrieve data, in this method we test all.
     * @id object to use for retrieval tests
     */
    private void testRetrieval(String id) throws IOException {
        long start;

        // 1. Retrieve as S3Object (so actual content not downloaded)
        start = System.nanoTime();
        try (S3Object s3Object = client.getObject(id)) {
            timingS3Object += (System.nanoTime() - start);
            // to avoid warnings from S3 we call abort
            s3Object.getObjectContent().abort();
            assertNotNull(s3Object);
            assertEquals(id, s3Object.getKey());
        }

        // 1b. Retrieve only metadata (so actual content not downloaded)
        start = System.nanoTime();
        ObjectMetadata metadata = client.getObjectMetadata(id);
        timingMetadata += (System.nanoTime() - start);
        assertNotNull(metadata);
        assertNotNull(metadata.getETag());

        // 3. Retrieve content as byte array (similar performance as method 4 using stream)
        start = System.nanoTime();
        byte[] content = client.getObjectContent(id);
        timingContentBytes += (System.nanoTime() - start);
        assertNotNull(content);
        assertEquals(TEST_OBJECT_DATA, new String(content));

        // 4. Retrieve content via stream (similar performance as method 3 using byte array)
        start = System.nanoTime();
        try (InputStream is = client.getObjectStream(id)) {
            String data = IOUtils.toString(is, StandardCharsets.UTF_8);
            timingContentStream += (System.nanoTime() - start);
            assertNotNull(is);
            assertEquals(TEST_OBJECT_DATA, data);
        }
        nrItems++;
    }

    @AfterAll
    public static void printTimings() {
        client.close();

        LOG.info("Time spend on retrieval of {} items...", nrItems);
        LOG.info("  S3Object (no content)  : {} ", timingS3Object / NANO_TO_MS);
        LOG.info("  Metadata (no content)  : {} ", timingMetadata / NANO_TO_MS);
        LOG.info("  Content as byte[] : {} ", timingContentBytes / NANO_TO_MS);
        LOG.info("  Content as stream : {}", timingContentStream / NANO_TO_MS);
    }

}
