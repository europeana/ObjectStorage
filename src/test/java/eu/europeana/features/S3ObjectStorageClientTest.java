package eu.europeana.features;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import eu.europeana.domain.StorageObject;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.System.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by jeroen on 18-12-16.
 */
@Ignore("TODO: fix ninja s3 server issue")
public class S3ObjectStorageClientTest {
    public static final String BUCKET_NAME = "europeana-sitemap-test";
    public static final String OBJECT_DATA_MD5 = "hfMGNWAtwJvYWVem6CosIQ==";
    public static final Integer EXPOSED_PORT = 9444;
    public static final String CLIENT_KEY = "AKIAIOSFODNN7EXAMPLE";
    public static final String SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    public static final String REGION = Regions.EU_CENTRAL_1.getName();
    private static GenericContainer s3server = new GenericContainer("meteogroup/s3mock:latest")
            .withExposedPorts(EXPOSED_PORT);
    private static int port;
    private static String host;
    private static boolean runInDocker = true;

    private static S3ObjectStorageClient client;

    @BeforeClass
    public static void initClientAndTestServer() {
        //TODO fix Amazon Mock S3 Container setup
        if (runInDocker) {
            s3server.start();
            port = s3server.getMappedPort(EXPOSED_PORT);
            host = s3server.getContainerIpAddress();
            client = new S3ObjectStorageClient(CLIENT_KEY, SECRET_KEY, BUCKET_NAME, "http://" + host + ":" + port + "/s3", new S3ClientOptions().withPathStyleAccess(true));
        } else {
            client = new S3ObjectStorageClient("some key", "some secret", "eu-central-1", "europeana-sitemap-test");
        }
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

    @Test
    public void testUploadKeyValueHappyFlow() throws Exception {
        final String OBJECT_NAME = "test-object";
        final String OBJECT_DATA = "object data";
        assertFalse(client.list().stream().map(StorageObject::getName).collect(Collectors.toList()).contains(OBJECT_NAME));
        Payload payload = new ByteArrayPayload(OBJECT_DATA.getBytes());
        String eid = client.put(OBJECT_NAME, payload);
        assertNotNull(eid);
        Optional<StorageObject> storageObject = client.get(OBJECT_NAME);
        assertTrue(storageObject.isPresent());
        StorageObject storageObjectValue = storageObject.get();
        assertEquals(OBJECT_NAME, storageObjectValue.getName());
        assertEquals(OBJECT_DATA, new String(getRawContent(storageObjectValue)));

        assertTrue(client.list().stream().map(StorageObject::getName).collect(Collectors.toList()).contains(OBJECT_NAME));

        Optional<StorageObject> storageObjectWithoutBody = client.getWithoutBody(OBJECT_NAME);
        assertTrue(storageObject.isPresent());
        StorageObject storageObjectWithoutBodyValue = storageObjectWithoutBody.get();
        assertEquals(OBJECT_NAME, storageObjectWithoutBodyValue.getName());
        assertEquals(0, getRawContent(storageObjectWithoutBodyValue).length);

        client.delete(OBJECT_NAME);
        assertFalse(client.list().stream().map(StorageObject::getName).collect(Collectors.toList()).contains(OBJECT_NAME));
    }

    //TODO test second put method
    //TODO test metadata check(length header has to be set)

    private byte[] getRawContent(StorageObject storageObject) {
        return ((ByteArrayPayload) storageObject.getPayload()).getRawContent();
    }


    @Test
    public void StorageObjectDoesNotExist() {
        Optional<StorageObject> storageObject = client.getWithoutBody("test");
        assertFalse(storageObject.isPresent());
    }

    @Test
    public void testList() {
        List<StorageObject> list = client.list();
        for (StorageObject so : list) {
            out.println(list.toString());
        }
    }

    @Ignore
    @Test
    public void downloadAndPrintSitemapFile() throws IOException {
        Optional<StorageObject> storageObject = client.get("europeana-sitemap-hashed-blue.xml?from=179999&to=224999");
        String rawContent = new String(getRawContent(storageObject.get()));
        System.out.println(rawContent);
    }

}