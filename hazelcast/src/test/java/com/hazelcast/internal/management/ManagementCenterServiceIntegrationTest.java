package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLDecoder;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ManagementCenterServiceIntegrationTest extends HazelcastTestSupport {

    private JettyServer jettyServer;
    private int portNum;

    @Before
    public void setUp() throws Exception {
        setLoggingLog4j();
        setLogLevel(Level.TRACE);

        URL root = new URL(MancenterServlet.class.getResource("/"), "../test-classes");
        String baseDir = URLDecoder.decode(root.getFile(), "UTF-8");
        String sourceDir = baseDir + "/../../src/test/webapp";
        String sourceName = "server_config.xml";
        portNum = availablePort();
        jettyServer = new JettyServer(portNum, sourceDir, sourceName);

        Hazelcast.newHazelcastInstance(getManagementCenterConfig());
    }

    @After
    public void tearDown() throws Exception {
        resetLogLevel();

        Hazelcast.shutdownAll();
        jettyServer.stop();
    }

    @Test
    public void testTimedMemberStateNotNull() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                CloseableHttpClient client = HttpClientBuilder.create().disableRedirectHandling().build();
                HttpUriRequest request = new HttpGet("http://localhost:" + portNum + "/mancen/memberStateCheck");
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

    private Config getManagementCenterConfig() {
        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl(format("http://localhost:%d%s/", portNum, "/mancen"));
        return config;
    }
}
