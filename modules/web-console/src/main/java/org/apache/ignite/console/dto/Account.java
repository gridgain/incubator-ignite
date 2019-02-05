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

package org.apache.ignite.console.dto;

import java.util.UUID;
import io.vertx.core.json.JsonObject;

/**
 * Data transfer object for this.
 */
public class Account extends AbstractDto {
    /** */
    private String email;

    /** */
    private String firstName;

    /** */
    private String lastName;

    /** */
    private String company;

    /** */
    private String country;

    /** */
    private String industry;

    /** */
    private boolean admin;

    /** */
    private String token;

    /** */
    private String resetPasswordToken;

    /** */
    private String registered;

    /** */
    private String lastLogin;

    /** */
    private String lastActivity;

    /** */
    private String lastEvent;

    /** */
    private boolean demoCreated;

    /** */
    private String salt;

    /** */
    private String hash;

    /**
     * Default constructor.
     */
    public Account() {
        
    }

    /**
     * Full construcor.
     *
     * @param id
     * @param email
     * @param firstName
     * @param lastName
     * @param company
     * @param country
     * @param industry
     * @param admin
     * @param token
     * @param resetPasswordToken
     * @param registered
     * @param lastLogin
     * @param lastActivity
     * @param lastEvent
     * @param demoCreated
     * @param salt
     * @param hash
     */
    public Account(
        UUID id,
        String email,
        String firstName,
        String lastName,
        String company,
        String country,
        String industry,
        boolean admin,
        String token,
        String resetPasswordToken,
        String registered,
        String lastLogin,
        String lastActivity,
        String lastEvent,
        boolean demoCreated,
        String salt,
        String hash
    ) {
        this.id = id;
        this.email = email;
        this.firstName = firstName;
        this.lastName = lastName;
        this.company = company;
        this.country = country;
        this.industry = industry;
        this.admin = admin;
        this.token = token;
        this.resetPasswordToken = resetPasswordToken;
        this.registered = registered;
        this.lastLogin = lastLogin;
        this.lastActivity = lastActivity;
        this.lastEvent = lastEvent;
        this.demoCreated = demoCreated;
        this.salt = salt;
        this.hash = hash;
    }

    /**
     * @return Account e-mail.
     */
    public String email() {
        return email;
    }

    /**
     * @return Account salt.
     */
    public String salt() {
        return salt;
    }

    /**
     * @return Account hash.
     */
    public String hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override public JsonObject toJson() {
        return super.toJson()
            .put("_id", id.toString())
            .put("email", email)
            .put("firstName", firstName)
            .put("lastName", lastName)
            .put("company", company)
            .put("country", country)
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
