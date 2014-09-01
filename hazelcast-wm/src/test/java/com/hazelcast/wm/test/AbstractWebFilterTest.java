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
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
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
import org.junit.AfterClass;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public abstract class AbstractWebFilterTest extends HazelcastTestSupport {

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

    protected static final String DEFAULT_MAP_NAME = "default";

    protected static final Map<Class<? extends AbstractWebFilterTest>, ContainerContext> CONTAINER_CONTEXT_MAP =
            new HashMap<Class<? extends AbstractWebFilterTest>, ContainerContext>();

    protected String serverXml1;
    protected String serverXml2;

    protected int serverPort1;
    protected int serverPort2;
    protected ServletContainer server1;
    protected ServletContainer server2;
    protected volatile HazelcastInstance hz;

    protected AbstractWebFilterTest(String serverXml1) {
        this.serverXml1 = serverXml1;
    }

    protected AbstractWebFilterTest(String serverXml1, String serverXml2) {
        this.serverXml1 = serverXml1;
        this.serverXml2 = serverXml2;
    }

    @Before
    public void setup() throws Exception {
        ContainerContext cc = CONTAINER_CONTEXT_MAP.get(getClass());
        // If container is not exist yet or
        // Hazelcast instance is not active (because of such as server shutdown)
        if (cc == null || isInstanceNotActive(cc.hz)) {
            // Build a new instance
            buildInstance();
            CONTAINER_CONTEXT_MAP.put(
                    getClass(),
                    new ContainerContext(
                            this,
                            serverXml1,
                            serverXml2,
                            serverPort1,
                            serverPort2,
                            server1,
                            server2,
                            hz));
            // If there is no previously created container
            // Instance maybe shutdown but container may be created before.
            // So "onTestStart" method must be called only for case that there is no container
            // Otherwise, it maybe called several times
            // since this code block is executed for every instance creation.
            if (cc == null) {
                onTestStart();
            }
            // New container created and started
            onContainerStart();
        } else {
            // For every test method a different test class can be constructed for parallel runs by JUnit.
            // So container can be exist, but configurations of current test may not be exist.
            // For this reason, we should copy container context information (such as ports, servers, ...)
            // to current test.
            cc.copyInto(this);
        }
        // Clear map
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        map.clear();
    }

    protected void buildInstance() throws Exception {
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
    }

    protected boolean isInstanceNotActive(HazelcastInstance hz) {
        Node node = TestUtil.getNode(hz);
        if (node == null) {
            return true;
        } else {
            return !node.isActive();
        }
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        for (Map.Entry<Class<? extends AbstractWebFilterTest>, ContainerContext> ccEntry :
                CONTAINER_CONTEXT_MAP.entrySet()) {
            ContainerContext cc = ccEntry.getValue();
            try {
                // Call "onTestFinish" methods of all tests.
                if (cc.test != null) {
                    cc.test.onTestFinish();
                }
                // Stop servers
                cc.server1.stop();
                if (cc.server2 != null) {
                    cc.server2.stop();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        // Shutdown all instances
        Hazelcast.shutdownAll();
    }

    // Called on test class startup
    protected void onTestStart() {

    }

    // Called on every container/hz instance creation
    protected void onContainerStart() {

    }

    // Called on test class terminate
    protected void onTestFinish() {

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
        HttpUriRequest request;
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
        return client.execute(request);
    }

    protected abstract ServletContainer getServletContainer(int port,
                                                            String sourceDir,
                                                            String serverXml) throws Exception;

    protected static class ContainerContext {

        protected AbstractWebFilterTest test;

        protected String serverXml1;
        protected String serverXml2;
        protected int serverPort1;
        protected int serverPort2;
        protected ServletContainer server1;
        protected ServletContainer server2;
        protected HazelcastInstance hz;

        public ContainerContext(AbstractWebFilterTest test,
                                String serverXml1,
                                String serverXml2,
                                int serverPort1,
                                int serverPort2,
                                ServletContainer server1,
                                ServletContainer server2,
                                HazelcastInstance hz) {
            this.test = test;
            this.serverXml1 = serverXml1;
            this.serverXml2 = serverXml2;
            this.serverPort1 = serverPort1;
            this.serverPort2 = serverPort2;
            this.server1 = server1;
            this.server2 = server2;
            this.hz = hz;
        }

        protected void copyInto(AbstractWebFilterTest awft) {
            awft.serverXml1 = serverXml1;
            awft.serverXml2 = serverXml2;
            awft.serverPort1 = serverPort1;
            awft.serverPort2 = serverPort2;
            awft.server1 = server1;
            awft.server2 = server2;
            awft.hz = hz;
        }

    }

}
