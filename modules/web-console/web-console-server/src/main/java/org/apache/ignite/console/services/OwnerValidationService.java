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

package org.apache.ignite.console.services;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Service;

/**
 * Service to check object ownership.
 */
@Service
public class OwnerValidationService {
    /** */
    private final Registry registry;

    /**
     * @param ignite Ignite.
     */
    public OwnerValidationService(Ignite ignite) {
        registry = new Registry(ignite, "wc_ownership_registry");
    }

    /**
     * Register link between account and object.
     *
     * @param accId Account ID.
     * @param objId Object ID.
     */
    public void register(UUID accId, UUID objId) {
        registry.process(accId, objId, true);
    }

    /**
     * Validate that link between account and object is correct.
     *
     * @param accId Account ID.
     * @param objId Object ID.
     */
    public void validate(UUID accId, UUID objId) {
        registry.process(accId, objId, false);
    }

    /**
     * @param objId Object ID.
     */
    public void unregister(UUID objId) {
        registry.unregister(objId);
    }

    /**
     * Special registry.
     */
    private static class Registry extends CacheHolder<UUID, UUID> {
        /**
         * @param ignite Ignite.
         * @param cacheName Cache name.
         */
        private Registry(Ignite ignite, String cacheName) {
            super(ignite, cacheName);
        }

        /**
         * Check that all operations with registry will be transactional.
         *
         * @throws IllegalStateException If active transaction not found.
         */
        private void checkTransaction() throws IllegalStateException {
            Transaction tx = ignite.transactions().tx();

            if (tx == null)
                throw new IllegalStateException("Active transaction not found");
        }

        /**
         * @param accId Account ID.
         * @param objId Object ID.
         * @param register {@code true} if link should be registered.
         * @throws IllegalStateException If failed to process link.
         */
        public void process(UUID accId, UUID objId, boolean register) throws IllegalStateException {
            checkTransaction();

            UUID oldAccId = cache.get(objId);

            if (oldAccId == null) {
                if (register)
                    cache.put(objId, accId);
            }
            else if (!oldAccId.equals(accId))
                throw new IllegalStateException("Data access violation");

        }

        /**
         * @param objId Object ID.
         * @throws IllegalStateException If failed to unregister link between account and object.
         */
        private void unregister(UUID objId) throws IllegalStateException  {
            checkTransaction();

            cache.remove(objId);
        }
    }
}
