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

package org.apache.ignite.console.model;

/**
 *
 */
public class Notification {
    private NotificationType type;
    private String subject;
    private String message;
    private Recipient recipient;

    public Notification(Recipient recipient, NotificationType type, String subject, String message) {
        this.type = type;
        this.subject = subject;
        this.message = message;
        this.recipient = recipient;
    }

    /**
     * @return Type.
     */
    public NotificationType getType() {
        return type;
    }

    /**
     * @param type New type.
     */
    public void setType(NotificationType type) {
        this.type = type;
    }

    /**
     * @return Subject.
     */
    public String getSubject() {
        return subject;
    }

    /**
     * @param subject New subject.
     */
    public void setSubject(String subject) {
        this.subject = subject;
    }

    /**
     * @return Message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param msg New message.
     */
    public void setMessage(String msg) {
        this.message = msg;
    }

    /**
     * @return Recipient.
     */
    public Recipient getRecipient() {
        return recipient;
    }

    /**
     * @param rcpt New recipient.
     */
    public void setRecipient(Recipient rcpt) {
        this.recipient = rcpt;
    }
}
