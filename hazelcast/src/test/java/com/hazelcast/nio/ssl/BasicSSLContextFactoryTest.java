package com.hazelcast.nio.ssl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getMalformedKeyStoreFilePath;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getWrongKeyStoreFilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicSSLContextFactoryTest {

    private BasicSSLContextFactory factory = new BasicSSLContextFactory();

    @Test
    public void testInit_withEmptyProperties() throws Exception {
        Properties properties = new Properties();

        factory.init(properties);

        assertSSLContext();
    }

    @Test
    public void testInit_withValidKeyStore() throws Exception {
        Properties properties = createSslProperties();

        factory.init(properties);

        assertSSLContext();
    }

    @Test
    public void testInit_withWrongKeyStore() throws Exception {
        Properties properties = createSslProperties();
        properties.setProperty(JAVAX_NET_SSL_KEY_STORE, getWrongKeyStoreFilePath());

        factory.init(properties);

        assertSSLContext();
    }

    @Test(expected = IOException.class)
    public void testInit_withMalformedKeyStore() throws Exception {
        Properties properties = createSslProperties();
        properties.setProperty(JAVAX_NET_SSL_KEY_STORE, getMalformedKeyStoreFilePath());

        factory.init(properties);
    }

    private void assertSSLContext() {
        SSLContext sslContext = factory.getSSLContext();
        assertNotNull(sslContext);
        assertEquals("TLS", sslContext.getProtocol());
    }
}
