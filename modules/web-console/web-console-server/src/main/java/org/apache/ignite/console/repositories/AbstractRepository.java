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

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.transactions.Transaction;

/**
 * Repository to work with notebooks.
 */
public abstract class AbstractRepository<T extends AbstractDto>  {
    /** */
    protected final Ignite ignite;

    /** */
    private final TransactionManager txMgr;

    /**
     * @param ignite Ignite.
     */
    protected AbstractRepository(Ignite ignite, TransactionManager txMgr) {
        this.ignite = ignite;
        this.txMgr = txMgr;
    }

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    public Transaction txStart() {
        return txMgr.txStart();
    }

    /**
     * Load short list of DTOs.
     *
     * @param accId Account ID.
     * @param ownerId Owner ID.
     * @param ownerIdx Index with DTOs IDs.
     * @param tbl Table with DTOs.
     */
    protected Collection<T> loadList(
        UUID accId,
        UUID ownerId,
        OneToManyIndex ownerIdx,
        Table<T> tbl
    ) {
        Collection<T> res;

        try (Transaction ignored = txStart()) {
            TreeSet<UUID> ids = ownerIdx.load(ownerId);

            res = tbl.loadAll(ids);
        }

        res.forEach(dto -> checkOwner(accId, dto));

        return res;
    }

    /**
     * @param accId Expected account ID.
     * @param dto Object to check.
     * @throws SecurityException If detected illegal access.
     */
    protected void checkOwner(UUID accId, AbstractDto dto) throws SecurityException {
        if (!accId.equals(dto.getAccountId()))
            throw new SecurityException("Illegal access to other user data");
    }
}
