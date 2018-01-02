/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package eu.europeana.domain;


import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.amazonaws.util.DateUtils.cloneDate;

/**
 * Represents the object metadata that is stored. This includes the standard HTTP headers (Content-Length, ETag, Content-MD5, etc.).
 */
public class ObjectMetadata implements Serializable {

    private static final long serialVersionUID = 309391172680920769L;

    /**
     * All other (non user custom) headers such as Content-Length, Content-Type,
     * etc.
     */
    private Map<String, Object> metadata;

    /**
     * Initialize a new objectmetadata
     */
    public ObjectMetadata() {
        metadata = new HashMap<>();
    }

    /**
     * Initialize a new objectmetadata
     * @param metadata map containing key-object metadata
     */
    public ObjectMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    /**
     * For internal use only. Sets a specific metadata header value. Not
     * intended to be called by external code.
     *
     * @param key   The name of the header being set.
     * @param value The value for the header.
     */
    public void setHeader(String key, Object value) {
        metadata.put(key, value);
    }

    /**
     * Gets a map of the raw metadata/headers for the associated object.
     *
     * @return A map of the raw metadata/headers for the associated object.
     */
    public Map<String, Object> getRawMetadata() {
        Map<String, Object> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copy.putAll(metadata);
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Returns the raw value of the metadata/headers for the specified key.
     * @param key
     * @return Object
     */
    public Object getRawMetadataValue(String key) {
        return metadata.get(key);
    }

    /**
     * Gets the value of the Last-Modified header, indicating the date
     * and time at which ObjectStorageClient last recorded a modification to the
     * associated object.
     *
     * @return The date and time at which
     * ObjectStorageClient last recorded a modification
     * to the associated object.
     */
    public Date getLastModified() {
        return cloneDate((Date) metadata.get(Headers.LAST_MODIFIED));
    }

    /**
     * For internal use only. Sets the Last-Modified header value
     * indicating the date and time at which ObjectStorageClient last recorded a
     * modification to the associated object.
     *
     * @param lastModified The date and time at which ObjectStorageClient last recorded a
     *                     modification to the associated object.
     */
    public void setLastModified(Date lastModified) {
        metadata.put(Headers.LAST_MODIFIED, lastModified);
    }

    /**
     * <p>
     * Gets the Content-Length HTTP header indicating the size of the
     * associated object in bytes.
     * </p>
     * <p>
     * This field is required when uploading objects to the storage provider, but the ObjectStorageClient Java
     * client will automatically set it when working directly with files. When
     * uploading directly from a stream, set this field if
     * possible. Otherwise the client must buffer the entire stream in
     * order to calculate the content length before sending the data to
     * ObjectStorageClient.
     * </p>
     * <p>
     * For more information on the Content-Length HTTP header, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>
     * </p>
     *
     * @return The Content-Length HTTP header indicating the size of the
     * associated object in bytes. Returns <code>null</code>
     * if it hasn't been set yet.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setContentLength(long)
     */
    public long getContentLength() {
        Long contentLength = (Long) metadata.get(Headers.CONTENT_LENGTH);

        if (contentLength == null) {
            return 0;
        }
        return contentLength.longValue();
    }

    /**
     * Returns the physical length of the entire object stored in S3.
     * This is useful during, for example, a range get operation.
     */
    public long getInstanceLength() {
        // See Content-Range in
        // http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
        String contentRange = (String) metadata.get(Headers.CONTENT_RANGE);
        if (contentRange != null) {
            int pos = contentRange.lastIndexOf('/');
            if (pos >= 0)
                return Long.parseLong(contentRange.substring(pos + 1));
        }
        return getContentLength();
    }

    /**
     * <p>
     * Sets the Content-Length HTTP header indicating the size of the
     * associated object in bytes.
     * </p>
     * <p>
     * This field is required when uploading objects to S3, but the ObjectStorageClient Java
     * client will automatically set it when working directly with files. When
     * uploading directly from a stream, set this field if
     * possible. Otherwise the client must buffer the entire stream in
     * order to calculate the content length before sending the data to
     * ObjectStorageClient.
     * </p>
     * <p>
     * For more information on the Content-Length HTTP header, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>
     * </p>
     *
     * @param contentLength The Content-Length HTTP header indicating the size of the
     *                      associated object in bytes.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getContentLength()
     */
    public void setContentLength(long contentLength) {
        metadata.put(Headers.CONTENT_LENGTH, contentLength);
    }

    /**
     * <p>
     * Gets the Content-Type HTTP header, which indicates the type of content
     * stored in the associated object. The value of this header is a standard
     * MIME type.
     * </p>
     * <p>
     * When uploading files, the ObjectStorageClient Java client will attempt to determine
     * the correct content type if one hasn't been set yet. Users are
     * responsible for ensuring a suitable content type is set when uploading
     * streams. If no content type is provided and cannot be determined by
     * the filename, the default content type, "application/octet-stream", will
     * be used.
     * </p>
     * <p>
     * For more information on the Content-Type header, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17</a>
     * </p>
     *
     * @return The HTTP Content-Type header, indicating the type of content
     * stored in the associated storage object. Returns <code>null</code>
     * if it hasn't been
     * set.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setContentType(String)
     */
    public String getContentType() {
        return (String) metadata.get(Headers.CONTENT_TYPE);
    }

    /**
     * <p>
     * Sets the Content-Type HTTP header indicating the type of content
     * stored in the associated object. The value of this header is a standard
     * MIME type.
     * </p>
     * <p>
     * When uploading files, the ObjectStorageClient Java client will attempt to determine
     * the correct content type if one hasn't been set yet. Users are
     * responsible for ensuring a suitable content type is set when uploading
     * streams. If no content type is provided and cannot be determined by
     * the filename, the default content type "application/octet-stream" will
     * be used.
     * </p>
     * <p>
     * For more information on the Content-Type header, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17</a>
     * </p>
     *
     * @param contentType The HTTP Content-Type header indicating the type of content
     *                    stored in the associated storage object.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getContentType()
     */
    public void setContentType(String contentType) {
        metadata.put(Headers.CONTENT_TYPE, contentType);
    }

    /**
     * <p>
     * Gets the Content-Language HTTP header, which describes the natural language(s) of the
     * intended audience for the enclosed entity.
     * </p>
     * <p>
     * For more information on the Content-Type header, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17</a>
     * </p>
     *
     * @return The HTTP Content-Language header, which describes the natural language(s) of the
     * intended audience for the enclosed entity. Returns <code>null</code>
     * if it hasn't been set.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setContentLanguage(String)
     */
    public String getContentLanguage() {
        return (String) metadata.get(Headers.CONTENT_LANGUAGE);
    }

    /**
     * <p>
     * Sets the Content-Language HTTP header which describes the natural language(s) of the
     * intended audience for the enclosed entity.
     * </p>
     * <p>
     * For more information on the Content-Type header, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17</a>
     * </p>
     *
     * @param contentLanguage The HTTP Content-Language header which describes the natural language(s) of the
     *                        intended audience for the enclosed entity.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getContentLanguage()
     */
    public void setContentLanguage(String contentLanguage) {
        metadata.put(Headers.CONTENT_LANGUAGE, contentLanguage);
    }

    /**
     * <p>
     * Gets the optional Content-Encoding HTTP header specifying what
     * content encodings have been applied to the object and what decoding
     * mechanisms must be applied in order to obtain the media-type referenced
     * by the Content-Type field.
     * </p>
     * <p>
     * For more information on how the Content-Encoding HTTP header works, see
     * <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>
     * </p>
     *
     * @return The HTTP Content-Encoding header.
     * Returns <code>null</code> if it hasn't been set.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setContentType(String)
     */
    public String getContentEncoding() {
        return (String) metadata.get(Headers.CONTENT_ENCODING);
    }

    /**
     * <p>
     * Sets the optional Content-Encoding HTTP header specifying what
     * content encodings have been applied to the object and what decoding
     * mechanisms must be applied in order to obtain the media-type referenced
     * by the Content-Type field.
     * </p>
     * <p>
     * For more information on how the Content-Encoding HTTP header works, see
     * <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>
     * </p>
     *
     * @param encoding The HTTP Content-Encoding header, as defined in RFC 2616.
     * @see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11"
     * >http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getContentType()
     */
    public void setContentEncoding(String encoding) {
        metadata.put(Headers.CONTENT_ENCODING, encoding);
    }

    /**
     * <p>
     * Gets the optional Cache-Control HTTP header which allows the user to
     * specify caching behavior along the HTTP request/reply chain.
     * </p>
     * <p>
     * For more information on how the Cache-Control HTTP header affects HTTP
     * requests and responses, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>
     * </p>
     *
     * @return The HTTP Cache-Control header as defined in RFC 2616.
     * Returns <code>null</code>  if
     * it hasn't been set.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setCacheControl(String)
     */
    public String getCacheControl() {
        return (String) metadata.get(Headers.CACHE_CONTROL);
    }

    /**
     * <p>
     * Sets the optional Cache-Control HTTP header which allows the user to
     * specify caching behavior along the HTTP request/reply chain.
     * </p>
     * <p>
     * For more information on how the Cache-Control HTTP header affects HTTP
     * requests and responses see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>
     * </p>
     *
     * @param cacheControl The HTTP Cache-Control header as defined in RFC 2616.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getCacheControl()
     */
    public void setCacheControl(String cacheControl) {
        metadata.put(Headers.CACHE_CONTROL, cacheControl);
    }

    /**
     * <p>
     * Sets the base64 encoded 128-bit MD5 digest of the associated object
     * (content - not including headers) according to RFC 1864. This data is
     * used as a message integrity check to verify that the data received by
     * ObjectStorageClient is the same data that the caller sent. If set to null,then the
     * MD5 digest is removed from the metadata.
     * </p>
     * <p>
     * This field represents the base64 encoded 128-bit MD5 digest digest of an
     * object's content as calculated on the caller's side. The ETag metadata
     * field represents the hex encoded 128-bit MD5 digest as computed by Amazon
     * S3.
     * </p>
     * <p>
     * The ObjectStorageClient Java client will attempt to calculate this field automatically
     * when uploading files to ObjectStorageClient.
     * </p>
     *
     * @param md5Base64 The base64 encoded MD5 hash of the content for the object
     *                  associated with this metadata.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getContentMD5()
     */
    public void setContentMD5(String md5Base64) {
        if (md5Base64 == null) {
            metadata.remove(Headers.CONTENT_MD5);
        } else {
            metadata.put(Headers.CONTENT_MD5, md5Base64);
        }

    }

    /**
     * <p>
     * Gets the base64 encoded 128-bit MD5 digest of the associated object
     * (content - not including headers) according to RFC 1864. This data is
     * used as a message integrity check to verify that the data received by
     * ObjectStorageClient is the same data that the caller sent.
     * </p>
     * <p>
     * This field represents the base64 encoded 128-bit MD5 digest digest of an
     * object's content as calculated on the caller's side. The ETag metadata
     * field represents the hex encoded 128-bit MD5 digest as computed by Amazon
     * S3.
     * </p>
     * <p>
     * The ObjectStorageClient Java client will attempt to calculate this field automatically
     * when uploading files to ObjectStorageClient.
     * </p>
     *
     * @return The base64 encoded MD5 hash of the content for the associated
     * object.  Returns <code>null</code> if the MD5 hash of the content
     * hasn't been set.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setContentMD5(String)
     */
    public String getContentMD5() {
        return (String) metadata.get(Headers.CONTENT_MD5);
    }

    /**
     * <p>
     * Sets the optional Content-Disposition HTTP header, which specifies
     * presentational information such as the recommended filename for the
     * object to be saved as.
     * </p>
     * <p>
     * For more information on how the Content-Disposition header affects HTTP
     * client behavior, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>
     * </p>
     *
     * @param disposition The value for the Content-Disposition header.
     * @see com.amazonaws.services.s3.model.ObjectMetadata#getContentDisposition()
     */
    public void setContentDisposition(String disposition) {
        metadata.put(Headers.CONTENT_DISPOSITION, disposition);
    }

    /**
     * <p>
     * Gets the optional Content-Disposition HTTP header, which specifies
     * presentation information for the object such as the recommended filename
     * for the object to be saved as.
     * </p>
     * <p>
     * For more information on how the Content-Disposition header affects HTTP
     * client behavior, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>
     * </p>
     *
     * @return The value of the Content-Disposition header.
     * Returns <code>null</code> if the Content-Disposition header
     * hasn't been set.
     * @see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1"
     * >http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>
     * @see com.amazonaws.services.s3.model.ObjectMetadata#setCacheControl(String)
     */
    public String getContentDisposition() {
        return (String) metadata.get(Headers.CONTENT_DISPOSITION);
    }

    /**
     * Gets the hex encoded 128-bit MD5 digest of the associated object
     * according to RFC 1864. This data is used as an integrity check to verify
     * that the data received by the caller is the same data that was sent by
     * ObjectStorageClient.
     * <p>
     * This field represents the hex encoded 128-bit MD5 digest of an object's
     * content as calculated by ObjectStorageClient. The ContentMD5 field represents the
     * base64 encoded 128-bit MD5 digest as calculated on the caller's side.
     * </p>
     *
     * @return The hex encoded MD5 hash of the content for the associated object
     * as calculated by ObjectStorageClient.
     * Returns <code>null</code> if it hasn't been set yet.
     */
    public String getETag() {
        return (String) metadata.get(Headers.ETAG);
    }


    public void setETag(String eTag) {
        metadata.put(Headers.ETAG, eTag);
    }


    /**
     * <p>
     * Returns the content range of the object if response contains the Content-Range header.
     * </p>
     * <p>
     * If the request specifies a range or part number, then response returns the Content-Range range header.
     * Otherwise, the response does not return Content-Range header.
     * </p>
     *
     * @return Returns content range if the object is requested with specific range or part number,
     * null otherwise.
     */
    public Long[] getContentRange() {
        String contentRange = (String) metadata.get(Headers.CONTENT_RANGE);
        Long[] range = null;
        if (contentRange != null) {
            String[] tokens = contentRange.split("[ -/]+");
            try {
                range = new Long[]{Long.valueOf(tokens[1]), Long.valueOf(tokens[2])};
            } catch (NumberFormatException nfe) {
                throw new ObjectStorageClientException(
                        "Unable to parse content range. Header 'Content-Range' has corrupted data" + nfe.getMessage(),
                        nfe);
            }
        }
        return range;
    }

    /**
     *
     * @param key
     * @param value
     */
    public void addMetaData(String key, Object value) {
        metadata.entrySet().add(new Map.Entry<String, Object>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                return value;
            }

            @Override
            public Object setValue(Object value) {
                return value;
            }
        });
    }

}
