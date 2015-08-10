package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(NightlyTest.class)
public class ManagementCenterServiceTest extends HazelcastTestSupport {

    private JettyServer jettyServer;
    private HazelcastInstance hazelcastInstance;
    private int portNum;

    @Before
    public void setUp() throws Exception {
        URL root = new URL(MancenterServlet.class.getResource("/"), "../test-classes");
        String baseDir = new File(root.getFile().replaceAll("%20", " ")).toString();
        String sourceDir = baseDir + "/../../src/test/webapp";
        String sourceName = "server_config.xml";
        portNum = availablePort();
        jettyServer = new JettyServer(portNum, sourceDir, sourceName);

        hazelcastInstance = Hazelcast.newHazelcastInstance(makeConfig());
    }

    @Test
    public void testTimedMemberStateNotNull() throws Exception {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CloseableHttpClient client = HttpClientBuilder.create().disableRedirectHandling().build();
                HttpUriRequest request;
                request = new HttpGet("http://localhost:" + portNum + "/mancen/memberStateCheck");
                HttpResponse response = client.execute(request);
                HttpEntity entity = response.getEntity();
                String responseString = EntityUtils.toString(entity);
                assertNotEquals("", responseString);
                JsonObject object = JsonObject.readFrom(responseString);
                TimedMemberState memberState = new TimedMemberState();
                memberState.fromJson(object);
                assertEquals("dev", memberState.getClusterName());
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstance.shutdown();
        jettyServer.stop();
    }

    private Config makeConfig() {
        Config config = new Config();
        return addManagementCenter(config);
    }

    private Config addManagementCenter(Config config) {
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl(format("http://localhost:%d%s/", portNum, "/mancen"));
        return config;
    }

    private int availablePort() throws IOException {
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
}
