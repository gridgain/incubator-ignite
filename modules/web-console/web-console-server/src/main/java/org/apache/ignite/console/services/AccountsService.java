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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.socket.WebSocketManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.notification.model.NotificationDescriptor.ACTIVATION_LINK;
import static org.apache.ignite.console.notification.model.NotificationDescriptor.PASSWORD_CHANGED;
import static org.apache.ignite.console.notification.model.NotificationDescriptor.PASSWORD_RESET;
import static org.apache.ignite.console.notification.model.NotificationDescriptor.WELCOME_LETTER;

/**
 * Service to handle accounts.
 */
@Service
public class AccountsService implements UserDetailsService {
    /** Authentication manager. */
    private final AuthenticationManager authMgr;

    /** */
    private final TransactionManager txMgr;

    /** */
    private final AccountsRepository accountsRepo;

    /** */
    private final WebSocketManager wsm;

    /** */
    private final NotificationService notificationSrvc;

    /** */
    private final PasswordEncoder encoder;

    /** */
    @Value("${app.activation.enabled}")
    private boolean activationEnabled;

    /**
     * @param authMgr Authentication manager.
     * @param txMgr Transactions manager.
     * @param accountsRepo Accounts repository.
     * @param wsm Websocket manager.
     * @param notificationSrvc Mail service.
     */
    public AccountsService(
        AuthenticationManager authMgr,
        TransactionManager txMgr,
        AccountsRepository accountsRepo,
        WebSocketManager wsm,
        NotificationService notificationSrvc
    ) {
        this.authMgr = authMgr;
        this.txMgr = txMgr;
        this.accountsRepo = accountsRepo;
        this.wsm = wsm;
        this.notificationSrvc = notificationSrvc;

        this.encoder = encoder();
    }

    /** {@inheritDoc} */
    @Override public Account loadUserByUsername(String email) throws UsernameNotFoundException {
        return accountsRepo.getByEmail(email);
    }

    /**
     * Create account for user.
     * 
     * @param params Sign up params.
     * @return Registered account.
     */
    Account create(SignUpRequest params) {
        return accountsRepo.create(
            new Account(
                params.getEmail(),
                encoder.encode(params.getPassword()),
                params.getFirstName(),
                params.getLastName(),
                params.getPhone(),
                params.getCompany(),
                params.getCountry()
            )
        );
    }

    /**
     * Register account for user.
     * 
     * @param params SignUp params.
     */
    public void register(SignUpRequest params) {
        Account acc = create(params);

        Authentication authentication = authMgr.authenticate(
            new UsernamePasswordAuthenticationToken(params.getEmail(), params.getPassword()));

        SecurityContextHolder.getContext().setAuthentication(authentication);

        notificationSrvc.sendEmail(acc.isEnabled() ? WELCOME_LETTER : ACTIVATION_LINK, acc);
    }

    /**
     * Delete account by ID.
     *
     * @return All registered accounts.
     */
    public List<Account> list() {
        return accountsRepo.list();
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     */
    Account delete(UUID accId) {
        return accountsRepo.delete(accId);
    }

    /**
     * Update admin flag..
     *
     * @param accId Account ID.
     * @param adminFlag New value for admin flag.
     */
    public void toggle(UUID accId, boolean adminFlag) {
        try (Transaction tx = txMgr.txStart()) {
            Account account = accountsRepo.getById(accId);

            if (account.getAdmin() != adminFlag) {
                account.setAdmin(adminFlag);

                accountsRepo.save(account);

                tx.commit();
            }
        }
    }

    /**
     * Save user.
     *
     * @param accId User ID.
     * @param changes Changes to apply to user.
     */
    public void save(UUID accId, ChangeUserRequest changes) {
        try (Transaction tx = txMgr.txStart()) {
            Account acc = accountsRepo.getById(accId);

            String pwd = changes.getPassword();

            if (!F.isEmpty(pwd))
                acc.setPassword(encoder.encode(pwd));

            String oldTok = acc.getToken();
            String newTok = changes.getToken();

            if (!oldTok.equals(newTok)) {
                wsm.revokeToken(oldTok);

                acc.setToken(newTok);
            }

            String oldEmail = acc.getEmail();
            String newEmail = changes.getEmail();

            if (!oldEmail.equals(newEmail)) {
                Account accByEmail = accountsRepo.getByEmail(oldEmail);

                if (acc.getId().equals(accByEmail.getId()))
                    acc.setEmail(changes.getEmail());
                else
                    throw new IllegalStateException("User with this email already registered");
            }

            acc.setFirstName(changes.getFirstName());
            acc.setLastName(changes.getLastName());
            acc.setPhone(changes.getPhone());
            acc.setCountry(changes.getCountry());
            acc.setCompany(changes.getCompany());

            accountsRepo.save(acc);

            tx.commit();
        }
    }

    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder encoder() {
        // Pbkdf2PasswordEncoder is compatible with passport.js, but BCryptPasswordEncoder is recommended by Spring.
        // We can return to Pbkdf2PasswordEncoder if we decided to import old users.
        //  Pbkdf2PasswordEncoder encoder = new Pbkdf2PasswordEncoder("", 25000, HASH_WIDTH); // HASH_WIDTH = 512
        //
        //  encoder.setAlgorithm(PBKDF2WithHmacSHA256);
        //  encoder.setEncodeHashAsBase64(true);

        return new BCryptPasswordEncoder();
    }

    /**
     * @param acc Account to check.
     * @throws IllegalStateException If account was not activated.
     */
    private void checkAccountActivated(Account acc) throws IllegalStateException {
        if (activationEnabled && !acc.activated())
            throw new IllegalStateException("Account was not activated by email: " + acc.getEmail());
    }

    /**
     * @param email User email to send reset password link.
     */
    public void forgotPassword(String email) {
        try (Transaction tx = txMgr.txStart()) {
            Account acc = accountsRepo.getByEmail(email);

            checkAccountActivated(acc);

            acc.setResetPasswordToken(UUID.randomUUID().toString());

            accountsRepo.save(acc);

            tx.commit();

            notificationSrvc.sendEmail(PASSWORD_RESET, acc);
        }
    }

    /**
     * @param email E-mail of user that request password reset.
     * @param resetPwdTok Reset password token.
     * @param newPwd New password.
     */
    public void resetPasswordByToken(String email, String resetPwdTok, String newPwd) {
        try (Transaction tx = txMgr.txStart()) {
            Account acc = accountsRepo.getByEmail(email);

            if (!resetPwdTok.equals(acc.getResetPasswordToken()))
                throw new IllegalStateException("Failed to find account with this token! Please check link from email.");

            checkAccountActivated(acc);

            acc.setPassword(encoder.encode(newPwd));
            acc.setResetPasswordToken(null);

            accountsRepo.save(acc);

            tx.commit();

            notificationSrvc.sendEmail(PASSWORD_CHANGED, acc);
        }
    }
}
