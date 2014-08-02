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

package com.hazelcast.wm.test;

import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Category(QuickTest.class)
public class TomcatWebFilterTest extends AbstractWebFilterTest {

    @Parameterized.Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{"node - not deferred", "node1-node.xml", "node2-node.xml"}, //
                new Object[]{"node - deferred", "node1-node-deferred.xml", "node2-node-deferred.xml"}, //
                new Object[]{"client - not deferred", "node1-client.xml", "node2-client.xml"}, //
                new Object[]{"client - deferred", "node1-client-deferred.xml", "node2-client-deferred.xml"} //
        );
    }

    public TomcatWebFilterTest(String name, String serverXml1, String serverXml2) {
        super(serverXml1,serverXml2);
    }

    @Test
    public void test_github_issue_2887() throws Exception
    {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);
        executeRequest("read", serverPort2, cookieStore);
        //expire session only on server2
        executeRequest("timeout", serverPort2, cookieStore);

        //Wait till session on server2 is expired
       sleepSeconds(2);

        //send redirect to server2 which has no local session but there is a distributed session.
        HttpResponse resp = request("redirect", serverPort2, cookieStore);

        assertEquals(302, resp.getStatusLine().getStatusCode());
    }


    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception {
        return new TomcatServer(port,sourceDir,serverXml);
    }
}
