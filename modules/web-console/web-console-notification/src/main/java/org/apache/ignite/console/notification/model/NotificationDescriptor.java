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

package org.apache.ignite.console.notification.model;

/**
 * Notification descriptors.
 */
public enum NotificationDescriptor implements INotificationDescriptor {
    /** */
    ADMIN_WELCOME_LETTER(
        "notifications.admin.welcome.letter.sbj",
        "notifications.admin.welcome.letter.msg"
    ),

    /** */
    WELCOME_LETTER(
        "notifications.welcome.letter.sbj",
        "notifications.welcome.letter.msg"
    ),

    /** */
    ACTIVATION_LINK(
        "notifications.activation.link.sbj",
        "notifications.activation.link.msg"
    ),

    /** */
    PASSWORD_RESET(
        "notifications.password.reset.sbj",
        "notifications.password.reset.msg"
    ),

    /** */
    PASSWORD_CHANGED(
        "notifications.password.changed.sbj",
        "notifications.password.changed.msg"
    ),

    /** */
    ACCOUNT_DELETED(
        "notifications.account.deleted.sbj",
        "notifications.account.deleted.msg"
    );

    /** */
    private final String sbjCode;

    /** */
    private final String msgCode;

    /**
     * @param sbjCode Subject code.
     * @param msgCode Message code.
     */
    NotificationDescriptor(String sbjCode, String msgCode) {
        this.sbjCode = sbjCode;
        this.msgCode = msgCode;
    }

    /** {@inheritDoc} */
    public String subjectCode() {
        return sbjCode;
    }

    /** {@inheritDoc} */
    public String messageCode() {
        return msgCode;
    }
}
