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

package org.apache.ignite.console.db.store;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.db.dto.Notebook;
import org.apache.ignite.console.db.index.OneToManyIndex;
import org.apache.ignite.console.db.index.UniqueIndex;
import org.apache.ignite.console.db.meta.Schemas;
import org.apache.ignite.transactions.Transaction;

/**
 * Store to work with Notebooks.
 */
@SuppressWarnings("JavaAbbreviationUsage")
public class NotebooksStore extends AbstractStore<UUID, Notebook> {
    /** */
    public static final String NOTEBOOKS_CACHE = "wc_notebooks";

    /** */
    private final OneToManyIndex accountNotebooksIdx;

    /** */
    private final UniqueIndex uniqueNotebookNameIdx;

    /**
     * @param ignite Ignite.
     */
    public NotebooksStore(Ignite ignite) {
        super(ignite, NOTEBOOKS_CACHE);

        accountNotebooksIdx = new OneToManyIndex(ignite, "account", "notebooks");
        uniqueNotebookNameIdx = new UniqueIndex(ignite, "uniqueNotebookNameIdx");
    }

    /**
     * TODO IGNITE-5617 Dirty hack to create caches.
     */
    @Override public NotebooksStore prepare() {
        super.prepare();

        accountNotebooksIdx.cache();
        uniqueNotebookNameIdx.cache();

        return this;
    }

    /** {@inheritDoc} */
    @Override public Collection<Notebook> list(JsonObject user) {
        UUID userId = getUserId(user);

        Collection<Notebook> notebooks;

        try(Transaction tx = txStart()) {
            TreeSet<UUID> notebookIds = accountNotebooksIdx.getIds(userId);

            notebooks = cache().getAll(notebookIds).values();

            tx.commit();
        }

        return notebooks;
    }

    /** {@inheritDoc} */
    @Override public Notebook put(JsonObject user, JsonObject rawData) {
        UUID userId = getUserId(user);

        rawData = Schemas.sanitize(Notebook.class, rawData);

        UUID notebookId = ensureId(rawData);

        String notebookName = rawData.getString("name");

        Notebook notebook = new Notebook(notebookId, null, notebookName, rawData.encode());

        try(Transaction tx = txStart()) {
            if (!uniqueNotebookNameIdx.putIfAbsent(userId, notebookName))
                throw new IllegalStateException("Notebook with name '" + notebookName + "' already exits");

            accountNotebooksIdx.put(userId, notebookId);

            tx.commit();
        }

        return notebook;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(JsonObject user, JsonObject rawData) {
        UUID userId = getUserId(user);

        UUID notebookId = getId(rawData);

        if (notebookId == null)
            throw new IllegalStateException("Notebook ID not found");

        boolean removed = false;

        try(Transaction tx = txStart()) {
            IgniteCache<UUID, Notebook> cache = cache();

            Notebook notebook = cache.getAndRemove(notebookId);

            if (notebook != null) {
                accountNotebooksIdx.remove(userId, notebookId);
                uniqueNotebookNameIdx.remove(userId, notebook.name());

                removed = true;
            }


            tx.commit();
        }

        return removed;
    }
}
