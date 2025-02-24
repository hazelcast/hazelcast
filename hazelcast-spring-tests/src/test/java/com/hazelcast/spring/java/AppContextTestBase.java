/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.java;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.JetService;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spring.ConfigCreator;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.spring.ExposeHazelcastObjects;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@SuppressWarnings("unused")
public abstract class AppContextTestBase
        extends HazelcastTestSupport {

    @Autowired
    private HazelcastInstance instance;

    @Autowired
    private JetService jet;

    @Autowired
    private SqlService sqlService;

    @Autowired(required = false)
    @Qualifier(value = "map1")
    protected IMap<String, String> map1;

    @Autowired(required = false)
    @Qualifier(value = "testMap")
    protected IMap<String, String> testMap;

    @Autowired
    private ApplicationContext applicationContext;

    @AfterAll
    static void stop() {
        HazelcastInstanceFactory.terminateAll();
        System.setProperty(ClusterProperty.METRICS_COLLECTION_FREQUENCY.getName(), "1");
    }

    @Test
    void testServices() {
        assertThat(instance).isNotNull();
        assertThat(jet).isNotNull();
        assertThat(sqlService).isNotNull();
    }

    @Test
    void testMap() {
        assertThat((Object) testMap).isNotNull();
    }

    @Configuration(proxyBeanMethods = false)
    @ExposeHazelcastObjects(excludeByName = "map1")
    public static class SpringHazelcastPartialConfiguration {

        @Bean
        public HazelcastInstance hazelcastInstance() {
            Config config = ConfigCreator.createConfig();
            config.setClusterName("spring-hazelcast-cluster-from-java");

            var mapConfig = new MapConfig("testMap");
            mapConfig.setBackupCount(2);
            mapConfig.setReadBackupData(true);
            config.addMapConfig(mapConfig);
            var map1Config = new MapConfig("map1");
            map1Config.setBackupCount(2);
            map1Config.setReadBackupData(true);
            config.addMapConfig(map1Config);
            return Hazelcast.newHazelcastInstance(config);
        }

        @Bean
        public PropertySourcesPlaceholderConfigurer propertyPlaceholderConfigurer() {
            var propertyPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
            propertyPlaceholderConfigurer.setLocation(new ClassPathResource("hazelcast-default.properties"));
            return propertyPlaceholderConfigurer;
        }
    }
}
