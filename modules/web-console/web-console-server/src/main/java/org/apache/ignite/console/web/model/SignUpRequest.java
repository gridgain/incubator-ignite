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

import javax.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Web model of sign up request.
 */
public class SignUpRequest {
    /** Email. */
    @NotNull
    @NotEmpty
    private String email;

    /** Password. */
    @NotNull
    @NotEmpty
    private String pwd;

    /** First name. */
    @NotNull
    @NotEmpty
    private String firstName;

    /** Last name. */
    @NotNull
    @NotEmpty
    private String lastName;

    /** Phone. */
    private String phone;

    /** Company. */
    @NotNull
    @NotEmpty
    private String company;

    /** Country. */
    @NotNull
    @NotEmpty
    private String country;

    /**
     * @return Email.
     */
    public String getEmail() {
        return email;
    }

    /**
     * @param email New email.
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * @return Password.
     */
    public String getPassword() {
        return pwd;
    }

    /**
     * @param pwd New password.
     */
    public void setPassword(String pwd) {
        this.pwd = pwd;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName New first name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName New last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Company.
     */
    public String getCompany() {
        return company;
    }

    /**
     * @param company New company.
     */
    public void setCompany(String company) {
        this.company = company;
    }

    /**
     * @return Country.
     */
    public String getCountry() {
        return country;
    }

    /**
     * @param country New country.
     */
    public void setCountry(String country) {
        this.country = country;
    }

    /**
     * @return Phone.
     */
    public String getPhone() {
        return phone;
    }

    /**
     * @param phone New phone.
     */
    public void setPhone(String phone) {
        this.phone = phone;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SignUpRequest.class, this);
    }
}
