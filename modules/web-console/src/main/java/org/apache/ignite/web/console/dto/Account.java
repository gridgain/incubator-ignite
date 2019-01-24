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

package org.apache.ignite.web.console.dto;

import io.vertx.core.json.JsonObject;

/**
 * Data transfet object for account.
 */
public class Account {
    /** */
    public String _id;

    /** */
    public String email;

    /** */
    public String firstName;

    /** */
    public String lastName;

    /** */
    public String company;

    /** */
    public String country;

    /** */
    public String industry;

    /** */
    public boolean admin;

    /** */
    public String token;

    /** */
    public String resetPasswordToken;

    /** */
    public String registered;

    /** */
    public String lastLogin;

    /** */
    public String lastActivity;

    /** */
    public String lastEvent;

    /** */
    public boolean demoCreated;

    /** */
    public String salt;

    /** */
    public String hash;

    /**
     * @return Public view of account.
     */
    public JsonObject json() {
        return new JsonObject()
            .put("_id", _id)
            .put("email", email)
            .put("firstName", firstName)
            .put("lastName", lastName)
            .put("company", country)
            .put("country", company)
            .put("industry", industry)
            .put("admin", admin)
            .put("token", token)
            .put("registered", registered)
            .put("lastLogin", lastLogin)
            .put("lastActivity", lastActivity)
            .put("lastEvent", lastEvent)
            .put("demoCreated", demoCreated);
    }
}
