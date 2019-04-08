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
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.console.db.NestedTransaction;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Repository to work with notebooks.
 */
public abstract class AbstractRepository<T extends AbstractDto>  {
    /** */
    protected final Ignite ignite;

    /** */
    private volatile boolean ready;

    /**
     * @param ignite Ignite.
     */
    protected AbstractRepository(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Initialize database.
     */
    protected abstract void initDatabase();

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    public Transaction txStart() {
        if (!ready) {
            initDatabase();

            ready = true;
        }

        IgniteTransactions txs = ignite.transactions();

        Transaction curTx = txs.tx();

        if (curTx instanceof NestedTransaction)
            return curTx;

        return curTx == null ? txs.txStart(PESSIMISTIC, REPEATABLE_READ) : new NestedTransaction(curTx);
    }

    /**
     * Load short list of DTOs.
     *
     * @param ownerId Owner ID.
     * @param ownerIdx Index with DTOs IDs.
     * @param tbl Table with DTOs.
     */
    protected Collection<T> loadList(
        UUID ownerId,
        OneToManyIndex ownerIdx,
        Table<T> tbl
    ) {
        try (Transaction ignored = txStart()) {
            TreeSet<UUID> ids = ownerIdx.load(ownerId);

            return tbl.loadAll(ids);
        }
    }
}
