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
public interface Headers {

    /*
     * Standard HTTP Headers
     */

    String CACHE_CONTROL = "Cache-Control";
    String CONTENT_DISPOSITION = "Content-Disposition";
    String CONTENT_ENCODING = "Content-Encoding";
    String CONTENT_LENGTH = "Content-Length";
    String CONTENT_RANGE = "Content-Range";
    String CONTENT_MD5 = "Content-MD5";
    String CONTENT_TYPE = "Content-Type";
    String CONTENT_LANGUAGE = "Content-Language";
    String DATE = "Date";
    String ETAG = "ETag";
    String LAST_MODIFIED = "Last-Modified";
    String SERVER = "Server";
    String CONNECTION = "Connection";

}