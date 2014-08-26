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

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Map.Entry;
import java.util.Random;

public abstract class AbstractWebFilterTest extends HazelcastTestSupport{

    public static boolean isBasicTest,isSetup;
    protected enum RequestType {

        GET_REQUEST,
        POST_REQUEST

    }

    static {
        final String logging = "hazelcast.logging.type";
        if (System.getProperty(logging) == null) {
            System.setProperty(logging, "log4j");
        }
        if (System.getProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK) == null) {
            System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");
        }
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");

        // randomize multicast group...
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);
    }

    protected static final String HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR = "::hz::";

    protected static final RequestType DEFAULT_REQUEST_TYPE = RequestType.GET_REQUEST;

    protected String serverXml1;
    protected String serverXml2;
    
    protected static int serverPort1;
    protected static int serverPort2;
    protected ServletContainer server1;
    protected ServletContainer server2;
    protected static HazelcastInstance hz;

    protected AbstractWebFilterTest(String serverXml1) {
        this.serverXml1 = serverXml1;
    }

    protected AbstractWebFilterTest(String serverXml1, String serverXml2) {
        this.serverXml1 = serverXml1;
        this.serverXml2 = serverXml2;
    }

    @Before
    public void setup() throws Exception {
        if(isBasicTest == true && isSetup == true){
            return;
        }
        final URL root = new URL(TestServlet.class.getResource("/"), "../test-classes");
        final String baseDir = new File(root.getFile().replaceAll("%20", " ")).toString();
        final String sourceDir = baseDir + "/../../src/test/webapp";
        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        serverPort1 = availablePort();
        server1 = getServletContainer(serverPort1, sourceDir, serverXml1);
        if (serverXml2 != null) {
            serverPort2 = availablePort();
            server2 = getServletContainer(serverPort2, sourceDir, serverXml2);
        }
        isSetup = true;
    }

    @After
    public void teardown() throws Exception {
        if(isBasicTest == true){
            return;
        }
        server1.stop();
        if (server2 != null) {
            server2.stop();
        }
        Hazelcast.shutdownAll();
    }

    protected int availablePort() throws IOException {
        while (true) {
            int port = (int) (65536 * Math.random());
            try {
                ServerSocket socket = new ServerSocket(port);
                socket.close();
                return port;
            } catch (Exception ignore) {
                // try next port
            }
        }
    }

    protected String findHazelcastSessionId(IMap<String, Object> map) {
        for (Entry<String, Object> entry : map.entrySet()) {
            if (!entry.getKey().contains(HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR)) {
                return entry.getKey();
            }
        }
        return null;
    }

    protected String responseToString(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }

    protected String executeRequest(String context,
                                    int serverPort,
                                    CookieStore cookieStore) throws Exception {
        return responseToString(request(context, serverPort, cookieStore));
    }

    protected HttpResponse request(String context,
                                   int serverPort,
                                   CookieStore cookieStore) throws Exception {
        return request(DEFAULT_REQUEST_TYPE, context, serverPort, cookieStore);
    }

    protected String executeRequest(RequestType reqType,
                                    String context,
                                    int serverPort,
                                    CookieStore cookieStore) throws Exception {
        return responseToString(request(reqType, context, serverPort, cookieStore));
    }

    protected HttpResponse request(RequestType reqType,
                                   String context,
                                   int serverPort,
                                   CookieStore cookieStore) throws Exception {
        if (reqType == null) {
            throw new IllegalArgumentException("Request type paramater cannot be empty !");
        }
        HttpClient client = HttpClientBuilder.create().disableRedirectHandling().setDefaultCookieStore(cookieStore).build();
        HttpUriRequest request = null;
        switch (reqType) {
            case GET_REQUEST:
                request = new HttpGet("http://localhost:" + serverPort + "/" + context);
                break;
            case POST_REQUEST:
                request = new HttpPost("http://localhost:" + serverPort + "/" + context);
                break;
            default:
                throw new IllegalArgumentException(reqType + " typed request is not supported");
        }
        HttpResponse response = client.execute(request);
        return response;
    }

    protected abstract ServletContainer getServletContainer(int port,
                                                            String sourceDir,
                                                            String serverXml) throws Exception;

}
