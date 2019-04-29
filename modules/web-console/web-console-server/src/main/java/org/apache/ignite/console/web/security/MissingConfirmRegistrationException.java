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

package org.apache.ignite.console.web.security;

import org.springframework.security.authentication.DisabledException;

/**
 * Thrown if an authentication request is rejected because the account is disabled.
 */
public class MissingConfirmRegistrationException extends DisabledException {
    /** User name. */
    private String userName;

    /**
     * @param msg Message.
     * @param userName User name.
     */
    public MissingConfirmRegistrationException(String msg, String userName) {
        super(msg);

        this.userName = userName;
    }

    /**
     * @return User name of disabled account.
     */
    public String getUserName() {
        return userName;
    }
}
