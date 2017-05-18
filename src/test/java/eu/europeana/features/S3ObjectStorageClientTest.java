package eu.europeana.features;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.domain.ContentValidationException;
import eu.europeana.domain.StorageObject;
import org.apache.commons.io.IOUtils;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class tests object storage and retrieval at Amazon S3.
 * For this test to work properly you need to place an objectstorage.properties in the src/main/test/resources folder
 * This file needs to contain the following keys that point to an existing bucket at S3 (s3.key, s3.secret, s3.region, s3.bucket).
 *
 * Created by Jeroen Jeurissen on 18-12-16
 * Updated by Patrick Ehlert on Feb 8th, 2017
 */
public class S3ObjectStorageClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectStorageClientTest.class);

    // docker configuration, doesn't work yet because of an issue to the Amazon Mock S3 container
    private static final String BUCKET_NAME = "europeana-sitemap-test";
    private static final Integer EXPOSED_PORT = 9444;
    private static final String CLIENT_KEY = "AKIAIOSFODNN7EXAMPLE";
    private static final String SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    private static final String REGION = Regions.EU_CENTRAL_1.getName();
    private static GenericContainer s3server;
    private static int port;
    private static String host;
    private static boolean runInDocker = false; // for now set to false

    private static final String TEST_BUCKET_NAME = "unit-test";

    private static final String TEST_OBJECT_NAME = "test-object";
    private static final String TEST_OBJECT_DATA = "object data";
    private static final String TEST_OBJECT_DATA_MD5 = "hfMGNWAtwJvYWVem6CosIQ==";

    private static S3ObjectStorageClient client;

    @BeforeClass
    public static void initClientAndTestServer() throws IOException {
        //TODO fix Amazon Mock S3 Container setup
        if (runInDocker) {
            s3server = new GenericContainer("meteogroup/s3mock:latest")
                    .withExposedPorts(EXPOSED_PORT);
            s3server.start();
            port = s3server.getMappedPort(EXPOSED_PORT);
            host = s3server.getContainerIpAddress();
            client = new S3ObjectStorageClient(CLIENT_KEY, SECRET_KEY, BUCKET_NAME, "http://" + host + ":" + port + "/s3", new S3ClientOptions().withPathStyleAccess(true));
        } else {
            Properties prop = loadAndCheckLoginProperties();
            client = new S3ObjectStorageClient(prop.getProperty("s3.key"), prop.getProperty("s3.secret"), prop.getProperty("s3.region"), prop.getProperty("s3.bucket"));
        }
    }

    private static Properties loadAndCheckLoginProperties() throws IOException {
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream("objectstorage.properties");
            if (in == null) {
                throw new RuntimeException("Please provide objectstorage.properties file with login details");
            }
            prop.load(in);
            // check if the properties contain login details for test and not production
            String bucketName = prop.getProperty("s3.bucket");
            if (bucketName != null && bucketName.contains("production")) {
                throw new RuntimeException("Do not use production settings for unit tests!");
            }
        } finally {
            IOUtils.closeQuietly(in);
        }
        return prop;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (runInDocker) {
            s3server.stop();
        }
    }

    @Before
    public void prepareTest() throws Exception {
        if (runInDocker) {
            Bucket bucket = client.createBucket(BUCKET_NAME);
        }
    }

    @After
    public void cleanUpTestData() {
        if (runInDocker) {
            client.deleteBucket(BUCKET_NAME);
        }
    }


    // TODO Fix test, for some reason we get a Access Denies when trying to list all buckets (or create a new bucket)
    // This has probably to do with the way we connect to S3
    //@Test
    public void testListBuckets() {
        for(Bucket bucket : client.listBuckets()) {
            LOG.info("Bucket: "+bucket.getName());
        }
    }

    /**
     * Check if our default test object already exists, if so it's probably from a previous test so we delete it
     */
    private void deleteOldTestObject() {
        Optional<StorageObject> leftover = client.getWithoutBody(TEST_OBJECT_NAME);
        if (leftover.isPresent())
        {
            LOG.warn("Found previous test object, deleting it.");
            client.delete(TEST_OBJECT_NAME);
        }
    }

    /**
     * We support 3 different methods to retrieve data, in this method we test all 3
     * @throws eu.europeana.domain.ContentValidationException
     */
    private void testRetrieval() throws ContentValidationException {
        // retrieve content as bytes
        byte[] content = client.getContent(TEST_OBJECT_NAME);
        assertNotNull(content);
        assertEquals(TEST_OBJECT_DATA, new String(content));

        // retrieve as storageobject without payload
        Optional<StorageObject> storageObjectWithoutBody = client.getWithoutBody(TEST_OBJECT_NAME);
        assertTrue(storageObjectWithoutBody.isPresent());
        StorageObject storageObjectWithoutBodyValue = storageObjectWithoutBody.get();
        assertEquals(TEST_OBJECT_NAME, storageObjectWithoutBodyValue.getName());
        assertEquals(0, getRawContent(storageObjectWithoutBodyValue).length);

        // retrieve as storageobject with payload without verification
        Optional<StorageObject> storageObject = client.get(TEST_OBJECT_NAME);
        assertTrue(storageObject.isPresent());
        StorageObject storageObjectValue = storageObject.get();
        assertEquals(TEST_OBJECT_NAME, storageObjectValue.getName());
        assertEquals(TEST_OBJECT_DATA, new String(getRawContent(storageObjectValue)));

        // retrieve as storageobject with payload with verification
        storageObject = client.get(TEST_OBJECT_NAME, true);
        assertTrue(storageObject.isPresent());
        storageObjectValue = storageObject.get();
        assertEquals(TEST_OBJECT_NAME, storageObjectValue.getName());
        assertEquals(TEST_OBJECT_DATA, new String(getRawContent(storageObjectValue)));

        // check if we can find it in list of objects, this make take quite some time
        //assertTrue(client.list().stream().map(StorageObject::getName).collect(Collectors.toList()).contains(TEST_OBJECT_NAME));
    }

    /**
     * Tests if we can put (using key and payload value), get and delete a simple object properly
     */
    @Test
    public void testUploadKeyPayload() throws ContentValidationException {
        deleteOldTestObject();

        Payload payload = new ByteArrayPayload(TEST_OBJECT_DATA.getBytes());
        String eid = client.put(TEST_OBJECT_NAME, payload);
        assertNotNull(eid);

        testRetrieval();

        // delete the object
        client.delete(TEST_OBJECT_NAME);
        assertFalse(client.get(TEST_OBJECT_NAME).isPresent());
    }

    /**
     * Test put (as storage object), get and delete of small test object
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    @Test
    public void testUploadStorageObject() throws ContentValidationException, IOException, URISyntaxException {
        deleteOldTestObject();

        // save test object
        Payload payload = new ByteArrayPayload(TEST_OBJECT_DATA.getBytes());
        StorageObject so = new StorageObject(TEST_OBJECT_NAME, new URL("http://www.europeana.eu/test/s3").toURI(), new Date(), null, payload);
        String eid = client.put(so);
        assertNotNull(eid);

        testRetrieval();

        // delete the object
        client.delete(TEST_OBJECT_NAME);
        assertFalse(client.get(TEST_OBJECT_NAME).isPresent());
    }

    /**
     * Does a stress test of putting, retrieving and deleting a small test object.
     * Note that this may take a while (approx. 6 1/2 minute for 1000 items)
     */
    @Test (timeout=500000)
    public void testStressUpload() throws ContentValidationException, IOException, URISyntaxException {
        deleteOldTestObject();

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
        return ((ByteArrayPayload) storageObject.getPayload()).getRawContent();
    }


    @Test
    public void testStorageObjectDoesNotExist() {
        Optional<StorageObject> storageObject = client.getWithoutBody("test");
        assertFalse(storageObject.isPresent());
    }

    /**
     * Simple test of listing all storage objects in the bucket.
     * This test may take some time, depending on how much data is present.
     */
    @Test
    public void testListObjects() {
        deleteOldTestObject();

        List<StorageObject> list = client.list();
        int count = list.size();
        LOG.info("Checking "+count+" storage objects...");
        int i = 0;
        for (StorageObject so : list) {
            assertNotNull(so);
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
        String rawContent = new String(getRawContent(storageObject.get()));
        System.out.println(rawContent);
    }

}