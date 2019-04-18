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

import org.apache.ignite.console.dto.Account;
import org.springframework.stereotype.Service;

/**
 * // TODO WC-940 Implement real e-mail service.
 */
@Service
public class MailService {
    /**
     * @param origin Request origin, required for composing reset link.
     * @param user User that requested password reset.
     */
    public void sendResetLink(String origin, Account user) {
        // TODO WC-940 Implement real e-mail service.

        String resetLink = origin + "/password/reset?token=" + user.resetPasswordToken();

        System.out.println("TODO WC-940 Send reset link [user=" + user.firstName() + " " + user.lastName() +
            ", email=" + user.email() + ", link=" + resetLink + "]");
    }

    /**
     * @param origin Request origin, required for composing reset link.
     * @param user User that requested password reset.
     */
    public void sendPasswordChanged(String origin, Account user) {
        // TODO WC-940 Implement real e-mail service.

        try {
            System.out.println("TODO WC-940 Your password has been changed [user=" + user.firstName() + " " + user.lastName() +
                ", email=" + user.email() + "]");

            String greeting = "Web Console"; // TODO WC-940 Move to settings.

            System.out.println("TODO WC-940 This is a confirmation that the password for your account on " +
                "<a href=" + origin + ">" + greeting + "</a> has just been changed.");
        }
        catch (Throwable ignored) {
            System.out.println("TODO WC-940 Password was changed, but failed to send confirmation email!");
        }
    }
}
