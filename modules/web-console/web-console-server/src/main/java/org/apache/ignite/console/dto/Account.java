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

import java.util.Collection;
import java.util.UUID;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * DTO for Account.
 */
public class Account extends AbstractDto implements UserDetails {
    /** Email. */
    private String email;

    /** Salt + hash for password. */
    private String hashedPwd;

    /** First name. */
    private String firstName;

    /** Last name. */
    private String lastName;

    /** Phone. */
    private String phone;

    /** Company. */
    private String company;

    /** Country. */
    private String country;

    /** Agent token. */
    private String tok;

    /** Reset password token. */
    private String resetPwdTok;

    /** Registered. */
    private String registered;

    /** Last login. */
    private String lastLogin;

    /** Last activity. */
    private String lastActivity;

    /** Last event. */
    private String lastEvt;

    /** Administration. */
    private boolean admin;

    /** Activated. */
    private boolean activated;

    /** Demo created. */
    private boolean demoCreated;

    /**
     * Default constructor for serialization.
     */
    public Account() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param email Email.
     * @param hashedPwd salt + hash for password.
     * @param firstName First name.
     * @param lastName Last name.
     * @param phone Phone.
     * @param company Company name.
     * @param country Country name.
     */
    public Account(
        String email,
        String hashedPwd,
        String firstName,
        String lastName,
        String phone,
        String company,
        String country
    ) {
        super(UUID.randomUUID());

        this.email = email;
        this.hashedPwd = hashedPwd;
        this.firstName = firstName;
        this.lastName = lastName;
        this.phone = phone;
        this.company = company;
        this.country = country;

        this.tok = UUID.randomUUID().toString();
        this.resetPwdTok = UUID.randomUUID().toString();
    }

    /**
     * @return First name.
     */
    public String firstName() {
        return firstName;
    }

    /**
     * @param firstName New first name.
     */
    public void firstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Last name.
     */
    public String lastName() {
        return lastName;
    }

    /**
     * @param lastName New last name.
     */
    public void lastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return e-mail.
     */
    public String email() {
        return email;
    }

    /**
     * @param email New email.
     */
    public void email(String email) {
        this.email = email;
    }

    /**
     * @return Phone.
     */
    public String phone() {
        return phone;
    }

    /**
     * @param phone New phone.
     */
    public void phone(String phone) {
        this.phone = phone;
    }

    /**
     * @return Company.
     */
    public String company() {
        return company;
    }

    /**
     * @param company New company.
     */
    public void company(String company) {
        this.company = company;
    }

    /**
     * @return Country.
     */
    public String country() {
        return country;
    }

    /**
     * @param country New country.
     */
    public void country(String country) {
        this.country = country;
    }

    /**
     * @return Token.
     */
    public String token() {
        return tok;
    }

    /**
     * @param tok New token.
     */
    public void token(String tok) {
        this.tok = tok;
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
     * @return Reset password token.
     */
    public String resetPasswordToken() {
        return resetPwdTok;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends GrantedAuthority> getAuthorities() {
        return null; // TODO IGNITE-5617 Implement or may be return empty collection.
    }

    /** {@inheritDoc} */
    @Override public String getUsername() {
        return email;
    }

    /** {@inheritDoc} */
    @Override public String getPassword() {
        return hashedPwd;
    }

    /** {@inheritDoc} */
    @Override public boolean isAccountNonExpired() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isAccountNonLocked() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isCredentialsNonExpired() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnabled() {
        return true;
    }
}
