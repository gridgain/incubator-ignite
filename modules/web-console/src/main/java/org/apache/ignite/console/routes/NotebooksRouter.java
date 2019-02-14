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

import java.util.UUID;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.UniqueIndex;
import org.apache.ignite.console.db.Schemas;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;

/**
 * Router to handle REST API for notebooks.
 */
public class NotebooksRouter extends AbstractRouter {
    /** */
    private final Table<Notebook> notebooksTbl;

    /** */
    private final OneToManyIndex notebooksIdx;

    /** */
    private final UniqueIndex notebookNameIdx;

    /**
     * @param ignite Ignite.
     */
    public NotebooksRouter(Ignite ignite) {
        super(ignite);

        notebooksTbl = new Table<>(ignite, "wc_notebooks");
        notebooksIdx = new OneToManyIndex(ignite, "wc_account_notebooks_idx");
        notebookNameIdx = new UniqueIndex(ignite, "wc_unique_notebook_name_idx", "Notebook '%s' already exits");
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        notebooksTbl.prepare();
        notebooksIdx.prepare();
        notebookNameIdx.prepare();
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/notebooks").handler(this::load);
        router.post("/api/v1/notebooks/save").handler(this::save);
        router.post("/api/v1/notebooks/remove").handler(this::delete);
    }

    /**
     * Load notebooks.
     *
     * @param ctx Context.
     */
    private void load(RoutingContext ctx) {
        loadList(ctx, notebooksTbl, notebooksIdx, "Failed to load notebooks");
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

                JsonObject json = Schemas.sanitize(Notebook.class, ctx.getBodyAsJson());

                UUID notebookId = ensureId(json);

                String name = json.getString("name");

                if (F.isEmpty(name))
                    throw new IllegalStateException("Notebook name is empty");

                Notebook notebook = new Notebook(notebookId, null, name, json.encode());

                try (Transaction tx = txStart()) {
                    notebookNameIdx.checkUnique(userId, name, notebook, notebook.name());

                    notebooksIdx.add(userId, notebookId);

                    notebooksTbl.save(notebook);

                    tx.commit();
                }

                sendResult(ctx, json);
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
    private void delete(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                UUID notebookId = getId(ctx.getBodyAsJson());

                if (notebookId == null)
                    throw new IllegalStateException("Notebook ID not found");

                int rmvCnt = 0;

                try (Transaction tx = txStart()) {
                    Notebook notebook = notebooksTbl.delete(notebookId);

                    if (notebook != null) {
                        notebooksIdx.remove(userId, notebookId);
                        notebookNameIdx.removeUniqueKey(userId, notebook.name());

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
