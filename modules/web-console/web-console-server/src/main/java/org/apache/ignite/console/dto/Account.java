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
 * DTO for Account.
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
    private String tok;

    /** */
    private String resetPwdTok;

    /** */
    private String registered;

    /** */
    private String lastLogin;

    /** */
    private String lastActivity;

    /** */
    private String lastEvt;

    /** */
    private String salt;

    /** */
    private String hash;

    /** */
    private boolean admin;

    /** */
    private boolean activated;

    /** */
    private boolean demoCreated;

    /**
     * @param json JSON data.
     * @return New instance of Account DTO.
     */
    public static Account fromJson(JsonObject json) {
        String id = json.getString("_id");

        if (id == null)
            throw new IllegalStateException("Account ID not found");

        return new Account(
            UUID.fromString(id),
            json.getString("email"),
            json.getString("firstName"),
            json.getString("lastName"),
            json.getString("company"),
            json.getString("country"),
            json.getString("industry"),
            json.getString("token"),
            json.getString("resetPasswordToken"),
            json.getString("registered"),
            json.getString("lastLogin"),
            json.getString("lastActivity"),
            json.getString("lastEvent"),
            json.getString("salt"),
            json.getString("hash"),
            json.getBoolean("admin", false),
            json.getBoolean("activated", false),
            json.getBoolean("demoCreated", false)
        );
    }

    /**
     * Default constructor for serialization.
     */
    public Account() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param email E_mail.
     * @param firstName First name.
     * @param lastName Last name.
     * @param company Company name.
     * @param country Country name.
     * @param industry Industry name.
     * @param tok Web agent token.
     * @param resetPwdTok Reset password token.
     * @param registered Registered flag.
     * @param lastLogin Last login date.
     * @param lastActivity  Last activity date.
     * @param lastEvt  Last event date.
     * @param salt Password salt.
     * @param hash Password hash.
     * @param admin Admin flag.
     * @param activated Activated flag.
     * @param demoCreated Demo created flag.
     */
    public Account(
        UUID id,
        String email,
        String firstName,
        String lastName,
        String company,
        String country,
        String industry,
        String tok,
        String resetPwdTok,
        String registered,
        String lastLogin,
        String lastActivity,
        String lastEvt,
        String salt,
        String hash,
        boolean admin,
        boolean activated,
        boolean demoCreated
    ) {
        super(id);

        this.email = email;
        this.firstName = firstName;
        this.lastName = lastName;
        this.company = company;
        this.country = country;
        this.industry = industry;

        this.tok = tok;
        this.resetPwdTok = resetPwdTok;

        this.registered = registered;
        this.lastLogin = lastLogin;
        this.lastActivity = lastActivity;
        this.lastEvt = lastEvt;

        this.salt = salt;
        this.hash = hash;

        this.admin = admin;
        this.activated = activated;
        this.demoCreated = demoCreated;
    }

    /**
     * @return First name.
     */
    public String firstName() {
        return firstName;
    }

    /**
     * @return Last name.
     */
    public String lastName() {
        return lastName;
    }

    /**
     * @return e-mail.
     */
    public String email() {
        return email;
    }

    /**
     * @return Company.
     */
    public String company() {
        return company;
    }

    /**
     * @return Country.
     */
    public String country() {
        return country;
    }

    /**
     * @return Admin flag.
     */
    public boolean admin() {
        return admin;
    }

    /**
     * @param admin Admin flag.
     */
    public void admin(boolean admin) {
        this.admin = admin;
    }

    /**
     * @return Activated flag.
     */
    public boolean activated() {
        return activated;
    }

    /**
     * @return Last login.
     */
    public String lastLogin() {
        return lastLogin;
    }

    /**
     * @return Last activity.
     */
    public String lastActivity() {
        return lastActivity;
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

    /**
     * @return Reset password token.
     */
    public String resetPasswordToken() {
        return resetPwdTok;
    }

    /**
     * @return JSON object with public fields.
     */
    public JsonObject publicView() {
        return new JsonObject()
            .put("_id", id.toString())
            .put("email", email)
            .put("firstName", firstName)
            .put("lastName", lastName)
            .put("company", company)
            .put("country", country)
            .put("industry", industry)
            .put("token", tok)
            .put("registered", registered)
            .put("lastLogin", lastLogin)
            .put("lastActivity", lastActivity)
            .put("lastEvent", lastEvt)
            .put("admin", admin)
            .put("activated", activated)
            .put("demoCreated", demoCreated);
    }

    /**
     * @return Account as JSON object.
     */
    public JsonObject toJson() {
        return new JsonObject()
            .put("_id", id.toString())
            .put("email", email)
            .put("firstName", firstName)
            .put("lastName", lastName)
            .put("company", company)
            .put("country", country)
            .put("industry", industry)

            .put("token", tok)
            .put("resetPasswordToken", resetPwdTok)

            .put("registered", registered)
            .put("lastLogin", lastLogin)
            .put("lastActivity", lastActivity)
            .put("lastEvent", lastEvt)

            .put("salt", salt)
            .put("hash", hash)

            .put("admin", admin)
            .put("activated", activated)
            .put("demoCreated", demoCreated);
    }
}
