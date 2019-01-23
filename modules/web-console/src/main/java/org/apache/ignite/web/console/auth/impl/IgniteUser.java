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

package org.apache.ignite.web.console.auth.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;

/**
 * User object.
 */
public class IgniteUser extends AbstractUser {
    /** */
    private final String userName;

    /** */
    private IgniteAuthImpl authProvider;

    /**
     * @param userName User name.
     */
    public IgniteUser(String userName) {
        this.userName = userName;
    }

    /** {@inheritDoc} */
    @Override protected void doIsPermitted(String perm, Handler<AsyncResult<Boolean>> asyncResHnd) {
        asyncResHnd.handle(Future.succeededFuture(true));
    }

    /** {@inheritDoc} */
    @Override public JsonObject principal() {
        return new JsonObject()
            .put("_id", "5683a8e9824d152c044e6281")
            .put("email", "kuaw26@mail.ru")
            .put("firstName", "Alexey")
            .put("lastName", "Kuznetsov")
            .put("company", "GridGain")
            .put("country", "Russia")
            .put("industry", "Other")
            .put("admin", true)
            .put("token", "NEHYtRKsPHhXT5rrIOJ4")
            .put("registered", "2017-12-21T16:14:37.369Z")
            .put("lastLogin", "2019-01-16T03:51:05.479Z")
            .put("lastActivity", "2019-01-16T03:51:06.084Z")
            .put("lastEvent", "2018-05-23T12:26:29.570Z")
            .put("demoCreated", "true");
    }

    /** {@inheritDoc} */
    @Override public void setAuthProvider(AuthProvider authProvider) {
        if (authProvider instanceof IgniteAuthImpl)
            this.authProvider = (IgniteAuthImpl)authProvider;
        else
            throw new IllegalArgumentException("Not a IgniteAuthImpl");
    }

    /**
     * @return User name.
     */
    public String getUserName() {
        return userName;
    }
}
