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

import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Dummy controller, just for migration to Java.
 */
@RestController
public class DummyController {
    /**
     * Dummy handler for: /api/v1/activities/page
     */
    @PostMapping(path = "/api/v1/activities/page", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonArray> dummyActivitiesPage(@RequestBody JsonObject params) {
        System.out.println("dummyActivitiesPage: " + params);

        return ResponseEntity.ok().build();
    }
}
