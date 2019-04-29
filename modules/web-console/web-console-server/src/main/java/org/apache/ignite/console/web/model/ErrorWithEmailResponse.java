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

/**
 * Error with email response.
 */
public class ErrorWithEmailResponse extends ErrorResponse {
    /** */
    private String email;

    /**
     * Default constructor for serialization.
     */
    public ErrorWithEmailResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param code Error code.
     * @param msg Error message.
     * @param username User name.
     */
    public ErrorWithEmailResponse(int code, String msg, String username) {
        super(code, msg);

        this.email = username;
    }

    /**
     * @return Email.
     */
    public String getEmail() {
        return email;
    }

    /**
     * @param email Email.
     */
    public void setEmail(String email) {
        this.email = email;
    }
}
