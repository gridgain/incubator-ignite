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
 * Web model of reset password request.
 */
public class ResetPasswordRequest {
    /** Email. */
    @NotNull
    @NotEmpty
    private String email;

    /** Password. */
    @NotNull
    @NotEmpty
    private String pwd;

    /** Reset password token. */
    @NotNull
    @NotEmpty
    private String tok;

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
     * @return Reset password token.
     */
    public String getToken() {
        return tok;
    }

    /**
     * @param tok Reset password token.
     */
    public void setToken(String tok) {
        this.tok = tok;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ResetPasswordRequest.class, this);
    }
}
