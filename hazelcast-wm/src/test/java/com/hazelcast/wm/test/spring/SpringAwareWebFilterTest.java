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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wm.test.JettyServer;
import com.hazelcast.wm.test.ServletContainer;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.springframework.context.ApplicationContext;
import org.springframework.security.core.session.SessionRegistry;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SpringAwareWebFilterTest extends SpringAwareWebFilterTestSupport {

    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception{
        return new JettyServer(port,sourceDir,serverXml);
    }

    @Test
    public void test_issue_3049() throws Exception {
        Set<ApplicationContext> applicationContextSet =
                SpringApplicationContextProvider.getApplicationContextSet();
        Iterator<ApplicationContext> i = applicationContextSet.iterator();
        ApplicationContext applicationContext1 = i.next();
        ApplicationContext applicationContext2 = i.next();
        SessionRegistry sessionRegistry1 = applicationContext1.getBean(SessionRegistry.class);
        SessionRegistry sessionRegistry2 = applicationContext2.getBean(SessionRegistry.class);

        SpringSecuritySession sss = login(null);

        request("hello.jsp", serverPort1, sss.cookieStore);

        String sessionId = sss.getSessionId();
        String hazelcastSessionId = sss.getHazelcastSessionId();

        assertTrue(
            "Native session must not exist in both Spring session registry of Node-1 and Node-2 after login",
            sessionRegistry1.getSessionInformation(sessionId) == null &&
                sessionRegistry2.getSessionInformation(sessionId) == null);

        assertTrue(
            "Hazelcast session must exist locally in one of the Spring session registry of Node-1 and Node-2 after login",
            sessionRegistry1.getSessionInformation(hazelcastSessionId) != null ||
                sessionRegistry2.getSessionInformation(hazelcastSessionId) != null);

        logout(sss);

        assertTrue(
            "Native session must not exist in both Spring session registry of Node-1 and Node-2 after logout",
            sessionRegistry1.getSessionInformation(sessionId) == null &&
                sessionRegistry2.getSessionInformation(sessionId) == null);

        assertTrue(
            "Hazelcast session must not exist in both Spring session registry of Node-1 and Node-2 after logout",
            sessionRegistry1.getSessionInformation(hazelcastSessionId) == null &&
                 sessionRegistry2.getSessionInformation(hazelcastSessionId) == null);
    }

}
