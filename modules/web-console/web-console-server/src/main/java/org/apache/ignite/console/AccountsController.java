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

package org.apache.ignite.console;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Customers controller.
 */
@RestController
@RequestMapping("/api/v1")
public class AccountsController {
    /** */
    @RequestMapping(path = "/user", method = RequestMethod.GET)
    public String getUser() {
        System.out.println("getUser");

        return "{\"_id\":\"5683a8e9824d152c044e6281\",\"email\":\"kuaw26@mail.ru\",\"firstName\":\"Alexey\",\"lastName\":\"Kuznetsov\",\"company\":\"GridGain\",\"country\":\"Russia\",\"industry\":\"Other\",\"admin\":true,\"token\":\"NEHYtRKsPHhXT5rrIOJ4\",\"registered\":\"2017-12-21T16:14:37.369Z\",\"lastLogin\":\"2019-03-24T15:07:44.215Z\",\"lastActivity\":\"2019-03-24T15:07:44.875Z\",\"lastEvent\":\"2018-05-23T12:26:29.570Z\",\"demoCreated\":true}";
    }
}
