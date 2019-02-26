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

'use strict';

const fs = require('fs');
const _ = require('lodash');
const nodemailer = require('nodemailer');

// Fire me up!

module.exports = {
    implements: 'services/mails',
    inject: ['settings']
};

/**
 * @param settings
 * @returns {MailsService}
 */
module.exports.factory = (settings) => {
    class MailsService {
        /**
         * Read template file.
         * @param {String} template Path to template file.
         * @param template
         */
        readTemplate(template) {
            try {
                return fs.readFileSync(template, 'utf8');
            }
            catch (ignored) {
                throw new Error('Failed to find email template: ' + template);
            }
        }

        /**
         * Prepare mail template.
         *
         * @param {String} template Path to template file.
         * @param ctx Mail context.
         * @return Prepared template.
         * @throws IOException If failed to prepare template.
         */
        prepareTemplate(template, ctx) {
            let content = this.readTemplate(template);

            _.forIn(ctx, (value, key) => content = content.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value || 'n/a'));

            return content;
        }

        /**
         * @param {string} subject Email subject.
         * @param {Account} user User that signed up.
         * @param {string} host Web Console host.
         */
        prepareContext(subject, user, host) {
            return {
                host,
                subject,
                greeting: settings.mail.greeting,
                sign: settings.mail.sign,
                firstName: user.firstName,
                lastName: user.lastName,
                email: user.email
            };
        }

        /**
         * Send mail to user.
         *
         * @param {String} template Path to template file.
         * @param {object} ctx
         * @throws {Error}
         * @return {Promise}
         */
        send(template, ctx) {
            const options = settings.mail;

            return new Promise((resolve, reject) => {
                if (_.isEmpty(options))
                    reject(new Error('SMTP server is not configured.'));

                if (!_.isEmpty(options.service)) {
                    if (_.isEmpty(options.auth) || _.isEmpty(options.auth.user) || _.isEmpty(options.auth.pass))
                        reject(new Error(`Credentials is not configured for service: ${options.service}`));
                }

                resolve(nodemailer.createTransport(options));
            })
                .then((transporter) => {
                    return transporter.verify().then(() => transporter);
                })
                .then((transporter) => {
                    return transporter.sendMail({
                        from: options.from,
                        to: `"${ctx.firstName} ${ctx.lastName}" <${ctx.email}>`,
                        subject: ctx.subject,
                        html: this.prepareTemplate(template, ctx)
                    });
                })
                .catch((err) => {
                    console.log('Failed to send email.', err);

                    return Promise.reject(err);
                });
        }

        /**
         * Send email when user signed up.
         *
         * @param host Web Console host.
         * @param user User that signed up.
         * @param createdByAdmin Whether user was created by admin.
         */
        sendWelcomeLetter(host, user, createdByAdmin) {
            const sbj = createdByAdmin ? `Account was created for ${settings.mail.greeting}.`
                : `Thanks for signing up for ${settings.mail.greeting}.`;

            const ctx = this.prepareContext(sbj, user, host);

            ctx.resetLink = `${host}/password/reset?token=${user.resetPasswordToken}`;
            ctx.reason = createdByAdmin ? 'administrator created account for you' : 'you have signed up';

            return this.send('templates/welcomeLetter.html', ctx);
        }

        /**
         * Send email to user for password reset.
         *
         * @param host
         * @param user
         */
        sendActivationLink(host, user) {
            const ctx = this.prepareContext(`Confirm your account on ${settings.mail.greeting}`, user, host);

            ctx.activationLink = `${host}/signin?activationToken=${user.activationToken}`;

            return this.send('templates/activationLink.html', ctx)
                .catch(() => Promise.reject(new Error('Failed to send email with confirm account link!')));
        }

        /**
         * Send email to user for password reset.
         *
         * @param host
         * @param user
         */
        sendResetLink(host, user) {
            const ctx = this.prepareContext('Password Reset', user, host);

            ctx.resetLink = `${host}/password/reset?token=${user.resetPasswordToken}`;

            return this.send('templates/resetPassword.html', ctx)
                .catch(() => Promise.reject(new Error('Failed to send email with reset link!')));
        }

        /**
         * Send email to user for password reset.
         * @param host
         * @param user
         */
        sendPasswordChanged(host, user) {
            const ctx = this.prepareContext('Password Reset', user, host);

            return this.send('templates/passwordChanged.html', ctx)
                .catch(() => Promise.reject(new Error('Password was changed, but failed to send confirmation email!')));
        }

        /**
         * Send email to user when it was deleted.
         * @param host
         * @param user
         */
        sendAccountDeleted(host, user) {
            const ctx = this.prepareContext('Your account was removed', user, host);

            return this.send('templates/accountDeleted.html', ctx)
                .catch(() => Promise.reject(new Error('Password was changed, but failed to send confirmation email!')));
        }
    }

    return new MailsService();
};
