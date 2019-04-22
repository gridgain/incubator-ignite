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

package org.apache.ignite.console.notification.services;

import java.io.IOException;
import java.util.Properties;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.notification.model.Notification;
import org.apache.ignite.console.notification.model.Recipient;
import org.apache.ignite.console.notification.config.MessagesProperties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.MessageSource;
import org.springframework.mail.javamail.JavaMailSender;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
public class MailServiceTest {
    /** Message source. */
    @Autowired
    private MessageSource msgSrc;

    /** JavaMail sender. */
    @Mock
    private JavaMailSender mailSnd;

    /** Argument capture  */
    @Captor
    private ArgumentCaptor<MimeMessage> captor;

    /** Mail service. */
    private MailService srvc;

    /** */
    @Before
    public void setup() {
        MessagesProperties cfg = new MessagesProperties();

        srvc = new MailService(msgSrc, mailSnd, cfg);

        when(mailSnd.createMimeMessage())
            .thenReturn(new MimeMessage(Session.getDefaultInstance(new Properties())));
    }

    @Test
    public void shouldSendEmail() throws MessagingException, IOException {
        Notification notification = new Notification(
            "http://test.com",
            new TestRecipient(),
            null
        );

//        notification.setSubject("subject");
//        notification.setMessage("text");

        srvc.send(notification);

        verify(mailSnd).send(captor.capture());

        MimeMessage msg = captor.getValue();
        
        assertEquals("subject", msg.getSubject());
        assertEquals("text", msg.getContent());
    }

    @Test
    public void shouldSendEmailWithExpressionInSubject() throws MessagingException, IOException {
        Notification notification = new Notification(
            "http://test.com",
            new TestRecipient(),
            null
        );

//        notification.setSubject("Hello ${recipient.firstName} ${recipient.lastName}! subject");
//        notification.setMessage("text");

        srvc.send(notification);

        verify(mailSnd).send(captor.capture());

        MimeMessage msg = captor.getValue();

        assertEquals("Hello firstName lastName! subject", msg.getSubject());
        assertEquals("text", msg.getContent());
    }

    /** */
    private static class TestRecipient implements Recipient {
        /** First name. */
        private String fn = "firstName";
        /** Last name. */
        public String lastName = "lastName";

        /** {@inheritDoc} */
        @Override public String getEmail() {
            return "test@test.com";
        }

        /** {@inheritDoc} */
        @Override public String getPhone() {
            return null;
        }

        /**
         * @return First name.
         */
        public String getFirstName() {
            return fn;
        }
    }
}
