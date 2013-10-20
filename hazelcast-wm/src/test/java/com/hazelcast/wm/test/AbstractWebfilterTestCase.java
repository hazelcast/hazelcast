package com.hazelcast.wm.test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public abstract class AbstractWebfilterTestCase {

    protected static final String HAZELCAST_SESSION_ATTRIBUTE_SEPERATOR = "::hz::";
    
    protected String serverXml1;
    protected String serverXml2;
    
    protected int serverPort1;
    protected int serverPort2;
    protected Server server1;
    protected Server server2;
    protected HazelcastInstance hz;

    @Before
    public void setup() throws Exception {
        hz = Hazelcast.newHazelcastInstance();
        final String baseDir = new File("").getAbsolutePath().replace("\\", "/");
        final String sourceDir = baseDir + (baseDir.toLowerCase().endsWith("target") ? "/../src/test/webapp" : "/src/test/webapp");
        serverPort1 = availablePort();
        serverPort2 = availablePort();
        server1 = buildServer(serverPort1, sourceDir, serverXml1);
        server2 = buildServer(serverPort2, sourceDir, serverXml2);
    }

    @After
    public void teardown() throws Exception {
        server1.stop();
        server2.stop();
        Hazelcast.shutdownAll();
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

    protected String findHazelcastSessionId(IMap<String, Object> map) {
        for (Entry<String, Object> entry : map.entrySet()) {
            if (!entry.getKey().contains(HAZELCAST_SESSION_ATTRIBUTE_SEPERATOR)) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    protected String executeRequest(String context, int serverPort, CookieStore cookieStore) throws Exception {
        HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();
        HttpGet request = new HttpGet("http://localhost:" + serverPort + "/" + context);
        HttpResponse response = client.execute(request);
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }

    private Server buildServer(int port, String sourceDir, String webXmlFile) throws Exception {
        Server server = new Server();

        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.addConnector(connector);

        WebAppContext context = new WebAppContext();
        context.setResourceBase(sourceDir);
        context.setDescriptor(sourceDir + "/WEB-INF/" + webXmlFile);
        context.setLogUrlOnStart(true);
        context.setContextPath("/");
        context.setParentLoaderPriority(true);

        server.setHandler(context);

        server.start();

        return server;
    }

}
