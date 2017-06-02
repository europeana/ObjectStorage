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

/**
 * Common HTTP header values used throughout the ObjectStorageClient Java client.
 */
public final class Headers {

    /*
     * Standard HTTP Headers
     */
    public static final String CACHE_CONTROL = "Cache-Control";
    public static final String CONTENT_DISPOSITION = "Content-Disposition";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_RANGE = "Content-Range";
    public static final String CONTENT_MD5 = "Content-MD5";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_LANGUAGE = "Content-Language";
    public static final String DATE = "Date";
    public static final String ETAG = "ETag";
    public static final String LAST_MODIFIED = "Last-Modified";
    public static final String SERVER = "Server";
    public static final String CONNECTION = "Connection";

    private Headers() {
        // empty constructor to prevent creation
    }
}
