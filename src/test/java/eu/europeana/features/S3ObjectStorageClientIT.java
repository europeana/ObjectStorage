package eu.europeana.features;

import com.amazonaws.services.s3.model.Bucket;
import eu.europeana.domain.ContentValidationException;
import eu.europeana.domain.StorageObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Integration test for testing (the speed of connections to) Amazon S3 and IBM S3 storage
 * For this test to work properly you need to place an objectstorage.IT.properties in the src/main/test/resources folder
 * This file needs to contain the following keys that point to an existing bucket at S3 (s3.key, s3.secret, s3.region, s3.bucket).
 *
 * Created by Jeroen Jeurissen on 18-12-16
 * Updated by Patrick Ehlert on Feb 8th, 2017
 */
public class S3ObjectStorageClientIT {

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClientIT.class);

    private static final int NANO_TO_SEC = 1000_000;

    private static boolean runBluemixTest = true;

    private static final String TEST_OBJECT_NAME = "test-object";
    private static final String TEST_OBJECT_DATA = "This is just some text...";
    //private static final String TEST_OBJECT_DATA_MD5 = "hfMGNWAtwJvYWVem6CosIQ==";

    private static S3ObjectStorageClient client;

    // we time the various retrieval methods to see which is fastest
    private static long nrItems = 0;

    private static long timingContentBytes = 0;
    private static long timingContentStream = 0;
    private static long timingMetadata = 0;
    private static long timingSONoPayload = 0;
    private static long timingSOPayloadVerify = 0;
    private static long timingSOPayloadNoVerify = 0;

    @BeforeClass
    public static void initClientAndTestServer() throws IOException {
        Properties prop = loadAndCheckLoginProperties();
        if (runBluemixTest) {
            client = new S3ObjectStorageClient(prop.getProperty("s3.key")
                    , prop.getProperty("s3.secret")
                    , prop.getProperty("s3.region")
                    , prop.getProperty("s3.bucket")
                    , prop.getProperty("s3.endpoint"));     // bluemix test
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

    // TODO Fix test, for some reason we get a Access Denies when trying to list all buckets (or create a new bucket)
    // This has probably to do with the way we connect to Amazon S3
    //@Test
    public void testListBuckets() {
        for(Bucket bucket : client.listBuckets()) {
            LOG.info("Bucket: "+bucket.getName());
        }
    }

    /**
     * Check if our default test object already exists, if so it's probably from a previous test so we delete it
     */
    private void deleteOldTestObject(String id) {
        if (client.isAvailable(id))
        {
            LOG.warn("Found previous test object, deleting it.");
            client.delete(id);
        }
    }

    /**
     * We support different methods to retrieve data, in this method we test all
     * @throws eu.europeana.domain.ContentValidationException
     */
    private void testRetrieval(String id) throws ContentValidationException, IOException {

        long start;
        Optional<StorageObject> optional;
        StorageObject storageObject;
        assertTrue("Can't test retrieval of object that is not available", client.isAvailable(id));

        // 1. retrieve content as bytes
        start = System.nanoTime();
        byte[] content2 = client.getContent(id);
        timingContentBytes += (System.nanoTime() - start);
        assertNotNull(content2);
        assertEquals(TEST_OBJECT_DATA, new String(content2));

        // 2. retrieve content as stream
        start = System.nanoTime();
        try (InputStream contentStream = client.getContentAsStream(id)) {
            byte[] content = contentStream.readAllBytes();
            timingContentStream += (System.nanoTime() - start);
            assertNotNull(content);
            assertEquals(TEST_OBJECT_DATA, new String(content));
        }

        // 3. retrieve metadata only
        start = System.nanoTime();
        eu.europeana.domain.ObjectMetadata metadata = client.getMetaData(id);
        timingMetadata += (System.nanoTime() - start);
        assertNotNull(metadata);

        // 4. retrieve as storageobject without payload
        start = System.nanoTime();
        optional = client.getWithoutBody(id);
        timingSONoPayload += (System.nanoTime() - start);

        assertTrue(optional.isPresent());
        storageObject = optional.get();
        assertEquals(id, storageObject.getName());
        assertNotNull(storageObject.getETag());
        assertNotNull(storageObject.getLastModified());
        assertEquals(0, getRawContent(storageObject).length);

        // 5. retrieve as storageobject with payload without verification
        start = System.nanoTime();
        optional = client.get(id);
        timingSOPayloadNoVerify += (System.nanoTime() - start);

        assertTrue(optional.isPresent());
        storageObject = optional.get();
        assertEquals(id, storageObject.getName());
        assertNotNull(storageObject.getETag());
        assertNotNull(storageObject.getLastModified());
        assertEquals(TEST_OBJECT_DATA, new String(getRawContent(storageObject)));

        // retrieve as storageobject with payload with verification
        start = System.nanoTime();
        optional = client.get(id, true);
        timingSOPayloadVerify += (System.nanoTime() - start);

        assertTrue(optional.isPresent());
        storageObject = optional.get();
        assertEquals(id, storageObject.getName());
        assertNotNull(storageObject.getETag());
        assertNotNull(storageObject.getLastModified());
        assertEquals(TEST_OBJECT_DATA, new String(getRawContent(storageObject)));

        // check if we can find it in list of objects, this make take quite some time
        //assertTrue(client.list().stream().map(StorageObject::getName).collect(Collectors.toList()).contains(id));
        nrItems++;
    }

    /**
     * Test what happens if an object does not exist
     */
    @Test
    public void testGetStorageObjectNotExist() throws ContentValidationException {
        deleteOldTestObject(TEST_OBJECT_NAME);

        Optional<StorageObject> optional = client.get("THIS_IS_NOT_A_VALID_OBJECT");
        assertFalse(optional.isPresent());

        Optional<StorageObject> optional2 = client.get("THIS_IS_NOT_A_VALID_OBJECT_TOO", true);
        assertFalse(optional2.isPresent());

        Optional<StorageObject> optional3 = client.getWithoutBody("THIS_IS_NOT_A_VALID_OBJECT_THREE");
        assertFalse(optional3.isPresent());
    }

    @Test
    public void getContentNotExists() {
        deleteOldTestObject(TEST_OBJECT_NAME);

        byte[] content = client.getContent("THIS_IS_NOT_A_VALID_OBJECT");
        assert(content.length == 0);
    }


    /**
     * Test what happens if the metadata of an object does not exist
     */
    @Test
    public void testGetMetaDataNotExist() {
        deleteOldTestObject(TEST_OBJECT_NAME);

        assertNull(client.getMetaData("THIS_IS_NOT_A_VALID_ID"));
    }

    /**
     * Tests if we can put (using key and payload value), get and delete a simple object properly
     */
    @Test
    public void testUploadKeyPayload() throws ContentValidationException, IOException {
        deleteOldTestObject(TEST_OBJECT_NAME);

        Payload payload = new ByteArrayPayload(TEST_OBJECT_DATA.getBytes());
        String eid = client.put(TEST_OBJECT_NAME, payload);
        assertNotNull(eid);

        testRetrieval(TEST_OBJECT_NAME);

        // delete the object
        client.delete(TEST_OBJECT_NAME);
        assertFalse(client.isAvailable(TEST_OBJECT_NAME));
    }

    /**
     * Test put (as storage object), get and delete of small test object
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    @Test
    public void testUploadStorageObject() throws ContentValidationException, IOException, URISyntaxException {
        deleteOldTestObject(TEST_OBJECT_NAME);

        // save test object
        Payload payload = new ByteArrayPayload(TEST_OBJECT_DATA.getBytes());
        StorageObject so = new StorageObject(TEST_OBJECT_NAME, new URL("http://www.europeana.eu/test/s3").toURI(), null, payload);
        String eid = client.put(so);
        assertNotNull(eid);

        testRetrieval(TEST_OBJECT_NAME);

        // delete the object
        client.delete(TEST_OBJECT_NAME);
        assertFalse(client.isAvailable(TEST_OBJECT_NAME));
    }

    /**
     * Does a stress test of putting, retrieving and deleting a small test object.
     * Note that this may take a while (approx. 5 minutes for 1000 items, or 33 sec for 100 items)
     */
    @Test (timeout=500000)
    public void testStressUpload() throws ContentValidationException, IOException, URISyntaxException {
        deleteOldTestObject(TEST_OBJECT_NAME);

        final int TEST_SIZE = 100;
        LOG.info("Starting stress test with size "+TEST_SIZE);
        for (int i = 0; i < TEST_SIZE ; i++) {
            // alternate between key/value upload and storage object upload
            if (i % 2 == 0) {
                testUploadKeyPayload();
            } else {
                testUploadStorageObject();
            }
            // provide feedback about progress
            if (i % 50 == 0) {
                LOG.info(String.format("%.1f", i * 100.0 / TEST_SIZE) +"%");
            }
        }
    }

    //TODO test metadata check(length header has to be set)

    private byte[] getRawContent(StorageObject storageObject) {
        if (storageObject.getPayload() != null) {
            return ((ByteArrayPayload) storageObject.getPayload()).getRawContent();
        }
        return new byte[0];
    }

    /**
     * Test if storageobjects confirm that an object is not present and if the isAvailable method returns false as expected
     */
    @Test
    public void testStorageObjectDoesNotExist() {
        assertFalse(client.isAvailable("NON_EXISTING_ID"));
        Optional<StorageObject> storageObject = client.getWithoutBody("NON_EXISTING_ID");
        assertFalse(storageObject.isPresent());
    }

    /**
     * Test if the storageObject equals function works properly. Note that some of the information
     * is only available after we saved an object
     */
    @Test
    public void testStorageObjectEquals() {
        deleteOldTestObject(TEST_OBJECT_NAME);

        // save test object
        Payload payload = new ByteArrayPayload(TEST_OBJECT_DATA.getBytes());
        StorageObject original = new StorageObject(TEST_OBJECT_NAME, null, null, payload);
        client.put(original);

        assertTrue(client.get(TEST_OBJECT_NAME).isPresent());
        StorageObject retrieved1 = client.get(TEST_OBJECT_NAME).get();
        assertNotEquals(original, retrieved1);
        StorageObject retrieved2 = client.get(TEST_OBJECT_NAME).get();
        assertEquals(retrieved1, retrieved2);

        // delete the object
        client.delete(TEST_OBJECT_NAME);
    }

    /**
     * Simple test of listing all storage objects in the bucket.
     * This test may take some time, depending on how much data is present.
     */
    @Test
    public void testListObjects() {
        deleteOldTestObject(TEST_OBJECT_NAME);

        List<StorageObject> list = client.list();
        int count = list.size();
        LOG.info("Checking "+count+" storage objects...");
        int i = 0;
        for (StorageObject so : list) {
            assertNotNull(so);
            assertNotNull(so.getName());
            assertNotNull(so.getLastModified());
            assertNotNull(so.getETag());
            // provide feedback about progress
            i++;
            if (i % 50 == 0) {
                LOG.info(String.format("%.1f", i * 100.0 / count) +"%");
            }
        }

        // add a new object and see if list count goes up with 1
        String eid = client.put(TEST_OBJECT_NAME, new ByteArrayPayload(TEST_OBJECT_DATA.getBytes()));
        assertNotNull(eid);
        list = client.list();
        assertEquals(count, list.size()-1);

        client.delete(TEST_OBJECT_NAME);
        list = client.list();
        assertEquals(count, list.size());
    }

    public void downloadAndPrintSitemapFile() throws IOException {
        Optional<StorageObject> storageObject = client.get("europeana-sitemap-hashed-blue.xml?from=179999&to=224999");
        assertTrue(storageObject.isPresent());
        String rawContent = new String(getRawContent(storageObject.get()));
        System.out.println(rawContent);
    }

    @AfterClass
    public static void printTimings() {
        LOG.info("Time spend on retrieval of {} items...", nrItems);
        LOG.info("  Content directly as bytes             : {} ", timingContentBytes / NANO_TO_SEC);
        LOG.info("  Content directly as bytes via stream  : {} ", timingContentStream / NANO_TO_SEC);
        LOG.info("  Metadata directly                     : {} ", timingMetadata / NANO_TO_SEC);
        LOG.info("  StorageObject no payload              : {} ", timingSONoPayload / NANO_TO_SEC);
        LOG.info("  StorageObject payload, no verification: {} ", timingSOPayloadNoVerify / NANO_TO_SEC);
        LOG.info("  StorageObject payload, verification   : {} ", timingSOPayloadVerify / NANO_TO_SEC);
    }

}
