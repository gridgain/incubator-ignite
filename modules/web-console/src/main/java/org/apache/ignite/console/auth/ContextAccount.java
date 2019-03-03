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

package org.apache.ignite.console.auth;

import java.util.UUID;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.IgniteUuidCache;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Account saved in session.
 */
public class ContextAccount extends AbstractUser {
    /** Account id. */
    private UUID accId;

    /** Cached account. */
    private Account cachedAccount;

    /** Auth provider. */
    private IgniteAuth authProvider;

    /**
     * Default constructor.
     */
    public ContextAccount() {
        // No-op.
    }

    /**
     * @param account Account.
     */
    ContextAccount(Account account) {
        this.accId = account.id();

        this.cachedAccount = account;
    }

    /** {@inheritDoc} */
    @Override protected void doIsPermitted(String perm, Handler<AsyncResult<Boolean>> asyncResHnd) {
        asyncResHnd.handle(Future.succeededFuture(true));
    }

    /** {@inheritDoc} */
    @Override public JsonObject principal() throws IgniteException {
        try {
            if (cachedAccount == null)
                cachedAccount = authProvider.account(accId);

            return cachedAccount.principal();
        }
        catch (IgniteAuthenticationException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public User clearCache() {
        cachedAccount = null;

        return super.clearCache();
    }

    /** {@inheritDoc} */
    @Override public void setAuthProvider(AuthProvider authProvider) throws IgniteException {
        if (authProvider instanceof IgniteAuth)
            this.authProvider = (IgniteAuth)authProvider;
        else
            throw new IgniteException("Not a " + IgniteAuth.class);
    }

    /** {@inheritDoc} */
    @Override public void writeToBuffer(Buffer buff) {
        super.writeToBuffer(buff);

        buff.appendLong(accId.getMostSignificantBits());
        buff.appendLong(accId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public int readFromBuffer(int pos, Buffer buf) {
        pos = super.readFromBuffer(pos, buf);

        long most = buf.getLong(pos);
        long least = buf.getLong(pos += Long.BYTES);

        accId = IgniteUuidCache.onIgniteUuidRead(new UUID(most, least));

        return pos + Long.BYTES;
    }
}
