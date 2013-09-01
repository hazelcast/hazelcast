package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.SSLSocketFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.*;

/**
 * @author mdogan 8/23/13
 */

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
@Ignore
public class SslSocketTest {

    private static String keyStore;
    private static String trustStore;

    @BeforeClass
    public static void init() throws IOException {
        File file = createTempFile("hazelcast.ks");
        keyStore = file.getAbsolutePath();
        file = createTempFile("hazelcast.ts");
        trustStore = file.getAbsolutePath();
    }

    private static File createTempFile(String resource) throws IOException {
        ClassLoader cl = SslSocketTest.class.getClassLoader();
        InputStream stream = cl.getResourceAsStream(resource);
        File file = File.createTempFile("hazelcast", "jks");
        OutputStream out = new FileOutputStream(file);
        int b;
        while ((b = stream.read()) > -1) {
            out.write(b);
        }
        out.close();
        stream.close();
        file.deleteOnExit();
        return file;
    }

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        System.setProperty("javax.net.ssl.keyStore", keyStore);
        System.setProperty("javax.net.ssl.trustStore", trustStore);
        System.setProperty("javax.net.ssl.keyStorePassword", "123456");

        System.err.println("keyStore = " + keyStore);
        try {
            Config cfg = new Config();
            cfg.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true));
            final HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

            ClientConfig config = new ClientConfig();
            config.addAddress("127.0.0.1");
            config.setRedoOperation(true);
            config.getSocketOptions().setSocketFactory(new SSLSocketFactory());

            final HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
            IMap<Object, Object> clientMap = client.getMap("test");

            for (int i = 0; i < 10; i++) {
                Assert.assertNull(clientMap.put(i, i));
            }

            IMap<Object, Object> map = hz.getMap("test");
            for (int i = 0; i < 10; i++) {
                Assert.assertEquals(i, map.get(i));
            }
        } finally {
            System.clearProperty("javax.net.ssl.keyStore");
            System.clearProperty("javax.net.ssl.trustStore");
            System.clearProperty("javax.net.ssl.keyStorePassword");
        }
    }

}
