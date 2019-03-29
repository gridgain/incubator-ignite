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

package org.apache.ignite.console.web.security;

import org.apache.ignite.IgniteCache;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;

/**
 * A {@link SessionRepository} backed by a Apache Ignite and that uses a {@link MapSession}.
 */
public class IgniteSessionRepository implements SessionRepository<ExpiringSession> {
    /**
     * If non-null, this value is used to override
     * {@link ExpiringSession#setMaxInactiveIntervalInSeconds(int)}.
     */
    private Integer dfltMaxInactiveInterval;

    /** Session cache. */
    private IgniteCache<String, MapSession> cache;

    /**
     * @param cache Session cache.
     */
    public IgniteSessionRepository(IgniteCache<String, MapSession> cache) {
        this.cache = cache;
    }

    /**
     * If non-null, this value is used to override
     * {@link ExpiringSession#setMaxInactiveIntervalInSeconds(int)}.
     * @param dfltMaxInactiveInterval the number of seconds that the {@link Session}
     * should be kept alive between client requests.
     */
    public IgniteSessionRepository setDefaultMaxInactiveInterval(int dfltMaxInactiveInterval) {
        this.dfltMaxInactiveInterval = dfltMaxInactiveInterval;
        
        return this;
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession createSession() {
        ExpiringSession res = new MapSession();
        
        if (this.dfltMaxInactiveInterval != null)
            res.setMaxInactiveIntervalInSeconds(this.dfltMaxInactiveInterval);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void save(ExpiringSession ses) {
        cache.put(ses.getId(), new MapSession(ses));
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession getSession(String id) {
        ExpiringSession saved = this.cache.get(id);

        if (saved == null)
            return null;

        if (saved.isExpired()) {
            delete(saved.getId());

            return null;
        }

        return saved;
    }

    /** {@inheritDoc} */
    @Override public void delete(String id) {
        cache.remove(id);
    }
}
