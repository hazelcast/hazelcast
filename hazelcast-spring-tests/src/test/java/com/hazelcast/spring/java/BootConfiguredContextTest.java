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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import static com.hazelcast.spring.ConfigCreator.createConfig;
import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = BootConfiguredContextTest.SpringHazelcastBootConfiguration.class)
public class BootConfiguredContextTest
        extends AppContextTestBase {

    @Test
    @Override
    void testMap() {
        assertThat((Object) map1).isNotNull();
        assertThat((Object) testMap).isNotNull();

        testMap.set("key1", "value1");
        assertEqualsEventually(() -> testMap.get("key1"), "value1");
    }

    @SpringBootApplication(scanBasePackages = "com.hazelcast.non.existing")
    public static class SpringHazelcastBootConfiguration {

        public static void main(String[] args) {
            SpringApplication.run(SpringHazelcastBootConfiguration.class, args);
        }

        @Bean(destroyMethod = "shutdown")
        public HazelcastInstance hazelcastInstance() {
            Config config = createConfig();
            config.setClusterName("spring-hazelcast-cluster-from-java");

            var mapConfig = new MapConfig("testMap");
            mapConfig.setBackupCount(2);
            mapConfig.setReadBackupData(true);
            config.addMapConfig(mapConfig);
            var map1Config = new MapConfig("map1");
            map1Config.setBackupCount(2);
            map1Config.setReadBackupData(true);
            config.addMapConfig(map1Config);
            return HazelcastInstanceFactory.newHazelcastInstance(config);
        }
    }
}
