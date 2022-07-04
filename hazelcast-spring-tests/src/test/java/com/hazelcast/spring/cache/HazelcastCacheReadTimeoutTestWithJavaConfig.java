/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.cache;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
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
public class HazelcastCacheReadTimeoutTestWithJavaConfig extends AbstractHazelcastCacheReadTimeoutTest {

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
            Config config = smallInstanceConfig();
            config.setClusterName("readtimeout-javaConfig");
            config.setProperty("hazelcast.graceful.shutdown.max.wait", "120");
            config.setProperty("hazelcast.partition.backup.sync.interval", "1");

            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getAutoDetectionConfig().setEnabled(false);

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
