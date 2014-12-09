/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.wm.test.spring;

import com.hazelcast.wm.test.AbstractWebFilterTest;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.springframework.web.context.support.WebApplicationContextUtils;

public abstract class SpringAwareWebFilterTestSupport extends AbstractWebFilterTest {

    protected static final String DEFAULT_SPRING_CONTEXT_FILE_PATH = "spring/hazelcast-spring.xml";

    protected static final String SPRING_SECURITY_LOGIN_URL = "j_spring_security_check";
    protected static final String SPRING_SECURITY_LOGOUT_URL = "j_spring_security_logout";
    protected static final String SPRING_SECURITY_LOGIN_USERNAME_PARAM = "j_username";
    protected static final String SPRING_SECURITY_LOGIN_PASSWORD_PARAM = "j_password";
    protected static final String SPRING_SECURITY_REMEMBER_ME_PARAM = "_spring_security_remember_me";

    protected static final String SPRING_SECURITY_DEFAULT_USERNAME = "user";
    protected static final String SPRING_SECURITY_DEFAULT_PASSWORD = "password";

    protected static final String SESSION_ID_COOKIE_NAME = "JSESSIONID";
    protected static final String HZ_SESSION_ID_COOKIE_NAME = "hazelcast.sessionId";
    protected static final String SPRING_SECURITY_COOKIE_NAME = "SPRING_SECURITY_REMEMBER_ME_COOKIE";

    public SpringAwareWebFilterTestSupport() {
        this(DEFAULT_SPRING_CONTEXT_FILE_PATH, DEFAULT_SPRING_CONTEXT_FILE_PATH);
    }

    protected SpringAwareWebFilterTestSupport(String serverXml1, String serverXml2) {
        super(serverXml1, serverXml2);
    }

    protected static class SpringSecuritySession {

        protected CookieStore cookieStore;
        protected HttpResponse lastResponse;

        protected SpringSecuritySession() {
            this.cookieStore = new BasicCookieStore();
        }

        protected SpringSecuritySession(CookieStore cookieStore) {
            this.cookieStore = cookieStore;
        }

        protected String getCookie(String cookieName) {
            for (Cookie cookie : cookieStore.getCookies()) {
                if (cookie.getName().equals(cookieName)) {
                    return cookie.getValue();
                }
            }
            return null;
        }

        protected String getSessionId() {
            return getCookie(SESSION_ID_COOKIE_NAME);
        }

        protected String getHazelcastSessionId() {
            return getCookie(HZ_SESSION_ID_COOKIE_NAME);
        }

        protected String getSpringSecurityCookie() {
            return getCookie(SPRING_SECURITY_COOKIE_NAME);
        }

    }

    protected SpringSecuritySession login(SpringSecuritySession springSecuritySession,
                                          boolean createSessionBeforeLogin) throws Exception {
        return login(springSecuritySession, SPRING_SECURITY_DEFAULT_USERNAME, SPRING_SECURITY_DEFAULT_PASSWORD,
                createSessionBeforeLogin);
    }

    protected SpringSecuritySession login(SpringSecuritySession springSecuritySession,
                                          String username,
                                          String password,
                                          boolean createSessionBeforeLogin) throws Exception {
        if (springSecuritySession== null) {
            springSecuritySession = new SpringSecuritySession();
        }

        if (createSessionBeforeLogin) {
            request(RequestType.POST_REQUEST,
                    SPRING_SECURITY_LOGIN_URL,
                    serverPort1, springSecuritySession.cookieStore);
        }

        HttpResponse response =
            request(
                RequestType.POST_REQUEST,
                SPRING_SECURITY_LOGIN_URL + "?" +
                    SPRING_SECURITY_LOGIN_USERNAME_PARAM + "=" + username + "&" +
                    SPRING_SECURITY_LOGIN_PASSWORD_PARAM + "=" + password,
                serverPort1, springSecuritySession.cookieStore);
        springSecuritySession.lastResponse = response;
        return springSecuritySession;
    }

    protected SpringSecuritySession logout(SpringSecuritySession springSecuritySession) throws Exception {
        if (springSecuritySession == null) {
            throw new IllegalArgumentException("SpringSecuritySession cannot be null !");
        }
        HttpResponse response =
            request(
                RequestType.POST_REQUEST,
                SPRING_SECURITY_LOGOUT_URL,
                serverPort1, springSecuritySession.cookieStore);
        springSecuritySession.lastResponse = response;
        return springSecuritySession;
    }

}
