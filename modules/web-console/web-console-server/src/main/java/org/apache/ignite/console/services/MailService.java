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

import org.springframework.stereotype.Service;

/**
 *
 */
@Service
public class MailService {
    /**
     * @param origin Request origin, required for composing reset link.
     * @param email Email to send reset link.
     * @param resetPwdTok Token to build reset link.
     */
    public void sendResetLink(String origin, String email, String resetPwdTok) {
        // TODO WC-940 TOD Implement real e-mail service.

        String resetLink = origin + "/password/reset?token=" + resetPwdTok;

        System.out.println("TODO WC-940 Send reset link: " + resetLink);
    }
}
