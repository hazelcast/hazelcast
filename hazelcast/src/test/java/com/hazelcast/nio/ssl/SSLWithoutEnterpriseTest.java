package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SSLWithoutEnterpriseTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void test() {
        Config config = new Config();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true)
                .setProperties(createSslProperties());

        config.getNetworkConfig().setSSLConfig(sslConfig);

        Hazelcast.newHazelcastInstance(config);
    }
}
