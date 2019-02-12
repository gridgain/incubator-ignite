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

package org.apache.ignite.console.routes;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.core.CacheHolder;
import org.apache.ignite.console.db.dto.JsonBuilder;
import org.apache.ignite.console.db.dto.Notebook;
import org.apache.ignite.console.db.index.OneToManyIndex;
import org.apache.ignite.console.db.index.UniqueIndex;
import org.apache.ignite.console.db.meta.Schemas;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;

/**
 * Router to handle REST API for notebooks.
 */
public class NotebooksRouter extends AbstractRouter {
    /** */
    private final CacheHolder<UUID, Notebook> notebooksTbl;

    /** */
    private final OneToManyIndex accountNotebooksIdx;

    /** */
    private final UniqueIndex uniqueNotebookNameIdx;

    /**
     * @param ignite Ignite.
     */
    public NotebooksRouter(Ignite ignite) {
        super(ignite);

        notebooksTbl = new CacheHolder<>(ignite, "wc_notebooks");
        accountNotebooksIdx = new OneToManyIndex(ignite, "wc_account_notebooks_idx");
        uniqueNotebookNameIdx = new UniqueIndex(ignite, "wc_unique_notebook_name_idx", "Notebook '%s' already exits");
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        notebooksTbl.prepare();
        accountNotebooksIdx.prepare();
        uniqueNotebookNameIdx.prepare();
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/notebooks").handler(this::load);
        router.post("/api/v1/notebooks/save").handler(this::save);
        router.post("/api/v1/notebooks/remove").handler(this::remove);
    }

    /**
     * Load notebooks.
     *
     * @param ctx Context.
     */
    private void load(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                try (Transaction tx = txStart()) {
                    TreeSet<UUID> notebookIds = accountNotebooksIdx.getIds(userId);

                    Collection<Notebook> notebooks = notebooksTbl.getAll(notebookIds).values();

                    tx.commit();

                    sendResult(ctx, new JsonBuilder().addArray(notebooks).buffer());
                }
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load notebooks", e);
            }
        }
    }

    /**
     * Save notebook.
     *
     * @param ctx Context.
     */
    private void save(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                JsonObject rawData = Schemas.sanitize(Notebook.class, ctx.getBodyAsJson());

                UUID notebookId = ensureId(rawData);

                String name = rawData.getString("name");

                if (F.isEmpty(name))
                    throw new IllegalStateException("Notebook name is empty");

                Notebook notebook = new Notebook(notebookId, null, name, rawData.encode());

                try (Transaction tx = txStart()) {
                    uniqueNotebookNameIdx.checkUnique(userId, name, notebook, notebook.name());

                    accountNotebooksIdx.putChild(userId, notebookId);

                    notebooksTbl.put(notebookId, notebook);

                    tx.commit();
                }

                sendResult(ctx, rawData);
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to save notebook", e);
            }
        }
    }

    /**
     * Remove notebook.
     *
     * @param ctx Context.
     */
    private void remove(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                UUID notebookId = getId(ctx.getBodyAsJson());

                if (notebookId == null)
                    throw new IllegalStateException("Notebook ID not found");

                int rmvCnt = 0;

                try (Transaction tx = txStart()) {
                    Notebook notebook = notebooksTbl.getAndRemove(notebookId);

                    if (notebook != null) {
                        accountNotebooksIdx.removeChild(userId, notebookId);
                        uniqueNotebookNameIdx.removeUniqueKey(userId, notebook.name());

                        rmvCnt = 1;
                    }

                    tx.commit();
                }

                sendResult(ctx, rowsAffected(rmvCnt));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to delete notebook", e);
            }
        }
    }
}
