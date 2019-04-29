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
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Properties;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.notification.config.MessageProperties;
import org.apache.ignite.console.notification.model.INotificationDescriptor;
import org.apache.ignite.console.notification.model.IRecipient;
import org.apache.ignite.console.notification.model.Notification;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.MessageSource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** */
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
public class MailServiceTest {
    /** Message source. */
    @Mock
    private MessageSource msgSrc;

    /** JavaMail sender. */
    @Mock
    private JavaMailSender mailSnd;

    /** Message properties. */
    @Mock
    MessageProperties cfg;

    /** Argument capture  */
    @Captor
    private ArgumentCaptor<MimeMessage> captor;

    /** Mail service. */
    @InjectMocks
    private MailService srvc;

    /** */
    @Before
    public void setup() {
        ReflectionTestUtils.setField(srvc, "from", "");
        
        when(msgSrc.getMessage(anyString(), isNull(Object[].class), anyString(), eq(Locale.US)))
            .thenAnswer(invocation -> invocation.getArguments()[2]);

        when(mailSnd.createMimeMessage())
            .thenReturn(new MimeMessage(Session.getDefaultInstance(new Properties())));
    }

    /** */
    @Test
    public void shouldSendEmail() throws MessagingException, IOException, URISyntaxException {
        INotificationDescriptor desc = new INotificationDescriptor() {
            @Override public String subjectCode() {
                return "subject";
            }

            @Override public String messageCode() {
                return "text";
            }
        };

        Notification notification = new Notification(
            "http://test.com",
            new TestRecipient(),
            desc
        );

        srvc.send(notification);

        verify(mailSnd).send(captor.capture());

        MimeMessage msg = captor.getValue();
        
        assertEquals("subject", msg.getSubject());
        assertEquals("text", msg.getContent());
    }

    /** */
    @Test
    public void shouldSendEmailWithExpressionInSubject() throws MessagingException, IOException, URISyntaxException {
        INotificationDescriptor desc = new INotificationDescriptor() {
            @Override public String subjectCode() {
                return "Hello ${recipient.firstName} ${recipient.lastName}! subject";
            }

            @Override public String messageCode() {
                return "text";
            }
        };

        Notification notification = new Notification(
            "http://test.com",
            new TestRecipient(),
            desc
        );

        srvc.send(notification);

        verify(mailSnd).send(captor.capture());

        MimeMessage msg = captor.getValue();

        assertEquals("Hello firstName lastName! subject", msg.getSubject());
        assertEquals("text", msg.getContent());
    }

    /** */
    private static class TestRecipient implements IRecipient {
        /** First name. */
        @SuppressWarnings("FieldCanBeLocal")
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
