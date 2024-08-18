package eu.europeana.features;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.BinaryUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for generating ObjectMetadata
 */
public final class MetadataUtils {

    private static final Logger LOG = LogManager.getLogger(MetadataUtils.class);

    private MetadataUtils() {
        // private constructor to prevent initialization
    }

    /**
     * Generate object metadata containing the contentLength and MD5 hash of the provided byte array
     * @param byteArray the byte array to read
     * @return new object metadata
     */
    public static ObjectMetadata generateObjectMetadata(byte[] byteArray) {
        ObjectMetadata metadata = new ObjectMetadata();
        try {
            byte[] md5binary = MessageDigest.getInstance("MD5").digest(byteArray);
            metadata.setContentMD5(BinaryUtils.toBase64(md5binary));
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Cannot calculate MD5 hash of because no MD5 algorithm was found", e);
        }
        metadata.setContentLength(byteArray.length);
        return metadata;
    }

    /**
     * Read the provided inputstream and generate object metadata containing the contentLength and MD5 hash.
     * Note that generating the metadata is a relatively expensive operation.
     * @param id optional, name of the object. Only displayed in case of errors
     * @param inputStream inputstream for which to generate ObjectMetaData
     * @return new object metadata
     */
    public static ObjectMetadata generateObjectMetadata(String id, InputStream inputStream) {
        ObjectMetadata metadata = new ObjectMetadata();
        byte[] content = new byte[0];
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (DigestInputStream dis = new DigestInputStream(inputStream, md)) {
                content = IOUtils.toByteArray(dis);
                metadata.setContentMD5(BinaryUtils.toBase64(md.digest()));
            }
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Cannot calculate MD5 hash of because no MD5 algorithm was found",e );
        } catch (IOException e) {
            LOG.error("Error reading inputStream for object {}", id, e);
        }
        Integer intLength = Integer.valueOf(content.length);
        metadata.setContentLength(intLength.longValue());
        return metadata;
    }
}
