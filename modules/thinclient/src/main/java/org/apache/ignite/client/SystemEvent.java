/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

/**
 * System events codes exposed by .
 */
public enum SystemEvent {
    /** Ignite unavailable. */IGNITE_UNAVAILABLE(10000),
    /** Protocol error. */PROTOCOL_ERROR(10001),
    /** Cache exists. */CACHE_EXISTS(10002),
    /** Cache not exist. */CACHE_DOES_NOT_EXIST(10003),
    /** Unsupported operation. */UNSUPPORTED_OPERATION(10004),
    /** Too many cursors. */TOO_MANY_CURSORS(10005),
    /** Server error. */SERVER_ERROR(10006),
    /** Protocol version mismatch. */PROTOCOL_VERSION_MISMATCH(10007),
    /** Cryptographic algorithm unavailable. */CRYPTO_ALGORITHM_UNAVAILABLE(10008),
    /** Key store provider not found. */KEYSTORE_TYPE_NOT_FOUND(10009),
    /** Key store file does not exist. */KEYSTORE_FILE_DOES_NOT_EXIST(10010),
    /** Generic SSL initialization error */SSL_INIT_ERROR(10011),
    /** Resource does not exist. */RESOURCE_DOES_NOT_EXIST(10012),
    /** Auth failed. */AUTH_FAILED(10013),
    /** Credential provider failed. */CRED_PROVIDER_FAILED(10014);

    /**
     * Constructor.
     */
    SystemEvent(int code) {
        this.code = code;
    }

    /**
     * @return Code.
     */
    public int getCode() {
        return code;
    }

    /** Code. */
    private final int code;
}
