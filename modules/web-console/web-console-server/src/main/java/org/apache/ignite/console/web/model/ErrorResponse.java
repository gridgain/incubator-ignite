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

package org.apache.ignite.console.web.model;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Error response JSON bean with the error code and technical error message.
 */
public class ErrorResponse {
    /** */
    private String msg;

    /**
     * Default constructor for serialization.
     */
    public ErrorResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param msg Error message.
     */
    public ErrorResponse(String msg) {
        this.msg = msg;
    }

    /**
     * @return Error message.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * @param errMsg Error message.
     */
    public void setMessage(String errMsg) {
        this.msg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ErrorResponse.class, this);
    }
}
