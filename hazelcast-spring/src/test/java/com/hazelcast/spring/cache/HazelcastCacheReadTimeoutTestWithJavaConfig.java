package com.hazelcast.spring.cache;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;

/**
 * Tests for {@link HazelcastCache} for timeout.
 *
 * @author Gokhan Oner
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = HazelcastCacheReadTimeoutTestWithJavaConfig.TestConfig.class)
@Category(QuickTest.class)
public class HazelcastCacheReadTimeoutTestWithJavaConfig extends AbstractHazelcastCacheReadTimeoutTest{

    @Configuration
    @EnableCaching
    @PropertySource("classpath:timeout.properties")
    static class TestConfig {

        @Bean
        HazelcastCacheManager cacheManager(HazelcastInstance hazelcastInstance) {
            return new HazelcastCacheManager(hazelcastInstance);
        }

        @Bean
        HazelcastInstance hazelcastInstance(Config config) {
            return Hazelcast.newHazelcastInstance(config);
        }

        @Bean
        Config config() {
            Config config = new Config();
            config.setProperty("hazelcast.wait.seconds.before.join", "0");
            config.setProperty("hazelcast.graceful.shutdown.max.wait", "120");
            config.setProperty("hazelcast.partition.backup.sync.interval", "1");

            config.getNetworkConfig().setPort(5701).setPortAutoIncrement(false);
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin()
                    .getTcpIpConfig()
                    .setEnabled(true)
                    .getMembers().add("127.0.0.1:5701");
            config.getNetworkConfig().getInterfaces()
                    .setEnabled(true)
                    .setInterfaces(Arrays.asList("127.0.0.1"));
            return config;
        }

        @Bean
        DummyTimeoutBean dummyTimeoutBean() {
            return new DummyTimeoutBean();
        }

        /**
         * Property placeholder configurer needed to process @Value annotations
         */
        @Bean
        public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
            return new PropertySourcesPlaceholderConfigurer();
        }

    }


    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

}