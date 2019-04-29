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

package org.apache.ignite.console.web.security;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;

import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;

/**
 * Security settings provider.
 */
@Configuration
@EnableWebSecurity
@EnableSpringHttpSession
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    /** Sign in route. */
    private static final String SIGN_IN_ROUTE = "/api/v1/signin";

    /** Sign up route. */
    private static final String SIGN_UP_ROUTE = "/api/v1/signup";

    /** Logout route. */
    private static final String LOGOUT_ROUTE = "/api/v1/logout";

    /** Forgot password route. */
    private static final String FORGOT_PASSWORD_ROUTE = "/api/v1/password/forgot";

    /** Reset password route. */
    private static final String RESET_PASSWORD_ROUTE = "/api/v1/password/reset";

    /** Public routes. */
    private static final String[] PUBLIC_ROUTES = new String[] {
        AGENTS_PATH,
        SIGN_IN_ROUTE, SIGN_UP_ROUTE, LOGOUT_ROUTE,
        FORGOT_PASSWORD_ROUTE, RESET_PASSWORD_ROUTE
    };

    /** */
    private final AccountsService accountsSrvc;
    
    /** */
    private final PasswordEncoder encoder;

    /**
     * @param encoder Service for encoding user passwords.
     * @param accountsSrvc User details service.
     */
    @Autowired
    public SecurityConfig(PasswordEncoder encoder, AccountsService accountsSrvc) {
        this.encoder = encoder;
        this.accountsSrvc = accountsSrvc;
    }

    /** {@inheritDoc} */
    @Override protected void configure(HttpSecurity http) throws Exception {
        http
            .csrf()
            .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
            .and()
            .authorizeRequests()
            .antMatchers(PUBLIC_ROUTES).anonymous()
            .antMatchers("/api/v1/admin/**").hasRole("ADMIN")
            .antMatchers("/api/v1/**").hasRole("USER")
            .anyRequest().authenticated()
            .and()
            .addFilterBefore(authenticationFilter(), UsernamePasswordAuthenticationFilter.class)
            .logout()
            .logoutUrl(LOGOUT_ROUTE)
            .deleteCookies("JSESSIONID")
            .logoutSuccessHandler(new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK));
    }

    /** {@inheritDoc} */
    @Override public void configure(WebSecurity web) {
        web.ignoring().antMatchers(
            "/v2/api-docs",
            "/configuration/ui",
            "/swagger-resources",
            "/configuration/security",
            "/swagger-ui.html",
            "/webjars/**"
        );
    }

    /** {@inheritDoc} */
    @Override protected void configure(AuthenticationManagerBuilder auth) {
        final DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider();

        authProvider.setUserDetailsService(accountsSrvc);
        authProvider.setPasswordEncoder(encoder);


        auth.authenticationProvider(authProvider);
    }

    /**
     * @param req Request.
     * @param res Response.
     * @param authentication Authentication.
     */
    private void loginSuccessHandler(
        HttpServletRequest req,
        HttpServletResponse res,
        Authentication authentication
    ) throws IOException {
        res.setStatus(HttpServletResponse.SC_OK);

        res.getWriter().flush();
    }

    /**
     * @param ignite Ignite.
     */
    @Bean
    public SessionRepository<ExpiringSession> sessionRepository(@Autowired Ignite ignite) {
        CacheConfiguration<String, MapSession> cfg = new CacheConfiguration<String, MapSession>()
            .setName("sessions")
            .setCacheMode(CacheMode.REPLICATED);

        return new IgniteSessionRepository(ignite.getOrCreateCache(cfg));
    }

    /**
     * Custom filter for retrieve credentials.
     */
    @Bean
    public RequestBodyReaderAuthenticationFilter authenticationFilter() throws Exception {
        RequestBodyReaderAuthenticationFilter authenticationFilter = new RequestBodyReaderAuthenticationFilter();

        authenticationFilter.setAuthenticationManager(authenticationManagerBean());
        authenticationFilter.setRequiresAuthenticationRequestMatcher(new AntPathRequestMatcher(SIGN_IN_ROUTE, "POST"));
        authenticationFilter.setAuthenticationSuccessHandler(this::loginSuccessHandler);

        return authenticationFilter;
    }
}
