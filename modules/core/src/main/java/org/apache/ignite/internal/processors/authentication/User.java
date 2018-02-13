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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 */
public class User implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default user name. */
    public static final String DFAULT_USER_NAME = "ignite";

    /** Default user password. */
    private static final String DFLT_USER_PASSWORD = "ignite";

    /** Salt length. */
    private static final int SALT_LEN = 64;

    /** Random. */
    private static final SecureRandom random = new SecureRandom();

    /** User name. */
    private String name;

    /** Encrypted password. */
    @GridToStringExclude
    private String encPasswd;

    /** Salt. */
    @GridToStringExclude
    private byte[] salt;

    /**
     * Constructor.
     */
    public User() {
    }

    /**
     * @param name User name.
     * @param encPasswd Encoded password.
     * @param salt Salt.
     */
    private User(String name, String encPasswd, byte[] salt) {
        this.name = name;
        this.encPasswd = encPasswd;
        this.salt = salt;
    }

    /**
     * @return User name.
     */
    public String name() {
        return name;
    }

    /**
     * Create new user.
     * @param name User name.
     * @param passwd Plain text password.
     * @return Created user.
     */
    public static User create(String name, String passwd) {
        byte [] salt = new byte[SALT_LEN];

        random.nextBytes(salt);

        return new User(name, password(passwd, salt), salt);
    }

    /**
     * Create empty user by login name.
     * @param name User name.
     * @return User.
     */
    public static User create(String name) {
        return new User(name, null, null);
    }

    /**
     * Create new user.
     *
     * @return Created user.
     */
    public static User defaultUser() {
        return create(DFAULT_USER_NAME, DFLT_USER_PASSWORD);
    }

    /**
     * @param passwd Plain text password.
     * @param salt Salt.
     * @return Hashed password.
     */
    @Nullable public static String password(String passwd, byte[] salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-512");

            for (int i = 0; i < 64; ++i) {
                md.update(passwd.getBytes(StandardCharsets.UTF_8));

                md.update(salt);
            }

            return Base64.getEncoder().encodeToString(md.digest(salt));
        } catch (NoSuchAlgorithmException e) {
            throw new IgniteException("Authentication error", e);
        }
    }

    /**
     * @param passwd Plain text password.
     * @return {@code true} If user authorized, otherwise returns {@code false}.
     */
    public boolean authorize(String passwd) {
        if (F.isEmpty(passwd))
            return encPasswd == null;

        return encPasswd.equals(password(passwd, salt));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        User u = (User)o;

        return F.eq(name, u.name) &&
            F.eq(salt, u.salt) &&
            F.eq(encPasswd, u.encPasswd);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(name, encPasswd, salt);
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(User.class, this);
    }
}
