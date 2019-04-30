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

import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Web model of the user.
 */
public class UserResponse extends User {
    /** Agent token. */
    @ApiModelProperty(value = "Agent token.")
    @NotNull
    @NotEmpty
    private String tok;

    /** Admin flag. */
    @ApiModelProperty(value = "Admin flag.")
    private boolean admin;

    /**
     * Default constructor for serialization.
     */
    public UserResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param email Email.
     * @param firstName First name.
     * @param lastName Last name.
     * @param phone Phone.
     * @param company Company.
     * @param country Country.
     * @param tok Agent token.
     * @param admin Admin flag.
     */
    public UserResponse(
        String email,
        String firstName,
        String lastName,
        String phone,
        String company,
        String country,
        String tok,
        boolean admin
    ) {
        super(email, firstName, lastName, phone, company, country);

        this.tok = tok;
        this.admin = admin;
    }

    /**
     * @return Agent token.
     */
    public String getToken() {
        return tok;
    }

    /**
     * @param tok New agent token.
     */
    public void setToken(String tok) {
        this.tok = tok;
    }

    /**
     * @return Admin flag.
     */
    public boolean isAdmin() {
        return admin;
    }

    /**
     * @param admin Admin flag.
     */
    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserResponse.class, this);
    }
}
