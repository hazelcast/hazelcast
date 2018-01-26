package com.hazelcast.security;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SecurityWithoutEnterpriseTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void test() {
        SecurityConfig securityConfig = new SecurityConfig()
                .setEnabled(true);

        Config config = new Config()
                .setSecurityConfig(securityConfig);

        createHazelcastInstance(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testSymmetricEncryption() {
        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true);
        Config config = new Config();
        config.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        createHazelcastInstance(config);
    }
}
