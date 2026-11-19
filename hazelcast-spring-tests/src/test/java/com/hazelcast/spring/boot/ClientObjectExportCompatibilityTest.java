/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.boot;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.ConfigCreator;
import com.hazelcast.spring.ExposeHazelcastObjects;
import jakarta.annotation.PreDestroy;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;

import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.waitClusterForSafeState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that Hazelcast clients are skipped when exporting objects, including in contexts that also contain a member.
 */
@TestPropertySource("classpath:test-params.properties")
public class ClientObjectExportCompatibilityTest {

    @Test
    void doesNotRunOnClient() {
        try (var ac = new AnnotationConfigApplicationContext(BasicClientObjectExportConfigurationBase.class)) {
            assertThat(ac.containsBean("hzInternalBeanExporter")).isTrue();
            assertThat(ac.getBean("client")).isInstanceOf(HazelcastClientProxy.class);
            assertThat(ac.containsBean("testMap")).isFalse();
        }
    }

    @Test
    void clientDoesNotInteractWhenCorrectMemberInContext() {
        try (var ac = new AnnotationConfigApplicationContext(ClientDoesNotMessMemberObjectExportConfiguration.class)) {
            assertThat(ac.getBean("client")).isInstanceOf(HazelcastClientProxy.class);
            HazelcastInstance member = ac.getBean("member", HazelcastInstance.class);
            IMap<?, ?> testMap = ac.getBean("testMap", IMap.class);
            assertThat((Object) testMap).isSameAs(member.getMap("testMap"));
        }
    }

    public static class ClientObjectExportConfigurationBase {
        private HazelcastInstance member;
        private HazelcastInstance client;

        @Bean(destroyMethod = "")
        public HazelcastInstance client() {
            var clusterName = randomName();
            createMember(clusterName);
            createClient(clusterName);

            return client;
        }

        protected void createClient(String clusterName) {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClusterName(clusterName);
            var address = member.getCluster().getLocalMember().getAddress();
            clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
            clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(30_000);
            client = HazelcastClient.newHazelcastClient(clientConfig);
        }

        protected void createMember(String clusterName) {
            Config config = ConfigCreator.createConfigWithStructures();
            config.setClusterName(clusterName);
            config.getNetworkConfig().getInterfaces().setEnabled(true);
            member = Hazelcast.newHazelcastInstance(config);
            waitClusterForSafeState(member);
        }

        @PreDestroy
        public void destroy() {
            try {
                if (client != null) {
                    client.shutdown();
                }
            } finally {
                if (member != null) {
                    member.getLifecycleService().terminate();
                }
            }
        }
    }

    @SpringBootApplication(scanBasePackages = "com.hazelcast.spring.boot.fortest")
    @ExposeHazelcastObjects
    public static class BasicClientObjectExportConfigurationBase extends ClientObjectExportConfigurationBase {

    }

    @SpringBootApplication(scanBasePackages = "com.hazelcast.spring.boot.fortest")
    @ExposeHazelcastObjects
    public static class ClientDoesNotMessMemberObjectExportConfiguration extends ClientObjectExportConfigurationBase {

        private final String clusterName = randomName();
        private HazelcastInstance member;
        private HazelcastInstance client;

        @Bean
        public HazelcastInstance member() {
            createMember(clusterName);
            return member;
        }

        @Bean @Primary
        @DependsOn("member")
        public HazelcastInstance client() {
            createClient(clusterName);
            return client;
        }

        protected void createClient(String clusterName) {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClusterName(clusterName);
            var address = member.getCluster().getLocalMember().getAddress();
            clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
            clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(30_000);
            client = HazelcastClient.newHazelcastClient(clientConfig);
        }

        protected void createMember(String clusterName) {
            Config config = ConfigCreator.createConfigWithStructures();
            config.setClusterName(clusterName);
            config.getNetworkConfig().getInterfaces().setEnabled(true);
            member = Hazelcast.newHazelcastInstance(config);
            waitClusterForSafeState(member);
        }

        @PreDestroy
        public void destroy() {
            try {
                if (client != null) {
                    client.shutdown();
                }
            } finally {
                if (member != null) {
                    member.getLifecycleService().terminate();
                }
            }
        }
    }

}
