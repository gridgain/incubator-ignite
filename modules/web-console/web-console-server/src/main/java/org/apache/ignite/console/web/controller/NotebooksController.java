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

package org.apache.ignite.console.web.controller;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.services.NotebooksService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for notebooks API.
 */
@RestController
@RequestMapping(path = "/api/v1/notebooks")
public class NotebooksController {
    /** */
    private final NotebooksService notebooksSrvc;

    /**
     * @param notebooksSrvc Notebooks service.
     */
    @Autowired
    public NotebooksController(NotebooksService notebooksSrvc) {
        this.notebooksSrvc = notebooksSrvc;
    }

    /**
     * @param user User.
     * @return
     */
    @GetMapping
    public ResponseEntity<Collection<Notebook>> list(@AuthenticationPrincipal Account user) {
        return ResponseEntity.ok(notebooksSrvc.list(user.getId()));
    }

    /**
     * @param user User.
     */
    @PutMapping(consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> save(@AuthenticationPrincipal Account user, @RequestBody Notebook notebook) {
        notebooksSrvc.save(user.getId(), notebook);

        return ResponseEntity.ok().build();
    }

    /**
     * @param user User.
     * @param notebookId Notebook ID.
     * @return Rows affected.
     */
    @DeleteMapping(path = "/{notebookId}")
    public ResponseEntity<JsonObject> delete(@AuthenticationPrincipal Account user, @PathVariable("notebookId") UUID notebookId) {
        ;

        return ResponseEntity.ok(notebooksSrvc.delete(user.getId(), notebookId));
    }
}
