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
import org.apache.ignite.console.notification.model.Notification;
import org.apache.ignite.console.notification.model.NotificationDescriptor;
import org.apache.ignite.console.notification.services.MailService;
import org.apache.ignite.console.web.socket.WebSocketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.common.Utils.currentRequestOrigin;

/**
 * Notification service.
 */
@Service
public class NotificationService {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketManager.class);

    /** Mail service. */
    private MailService mailSrvc;

    /**
     * @param srvc Mail service.
     */
    public NotificationService(MailService srvc) {
        mailSrvc = srvc;
    }

    /**
     * @param desc Notification description.
     * @param acc Account.
     */
    public void sendEmail(NotificationDescriptor desc, Account acc) {
        try {
            Notification notification = new Notification(currentRequestOrigin(), acc, desc);

            mailSrvc.send(notification);
        }
        catch (Throwable e) {
            log.error("Failed to send email", e);
        }
    }
}
