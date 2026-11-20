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

package com.hazelcast.spring;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObjectExportRegressionTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "named")
    void matchingMemberShouldNotInitializeAnUnrelatedLazyClient(String instanceName) {
        var factory = new DefaultListableBeanFactory();
        var member = member("named", "cluster");
        var client = new RootBeanDefinition(HazelcastInstance.class, () -> {
            throw new IllegalStateException("Unrelated lazy client was initialized");
        });
        client.setLazyInit(true);
        factory.registerBeanDefinition("unrelatedClient", client);
        register(factory, "member", member, true);
        factory.getBean("member");
        var creator = new BeanCreator(factory);
        assertThat(creator.createBean("orders", (hz, name) -> hz, instanceName, "cluster")).isSameAs(member);
        assertThat(factory.containsSingleton("unrelatedClient")).isFalse();
    }

    @Test
    void primaryShouldBeResolvedWithinTheMatchingCluster() {
        var factory = new DefaultListableBeanFactory();
        var first = member(null, "cluster-a");
        register(factory, "primaryA", first, true);
        register(factory, "secondaryA", member(null, "cluster-a"), false);
        register(factory, "primaryB", member(null, "cluster-b"), true);
        var creator = new BeanCreator(factory);
        assertThat(creator.createBean("orders", (hz, name) -> hz, null, "cluster-a")).isSameAs(first);
    }

    @Test
    void primaryFromMatchingParentClusterShouldNotBeHiddenByChildPrimary() {
        var parent = new DefaultListableBeanFactory();
        var first = member(null, "cluster-a");
        register(parent, "primaryA", first, true);
        register(parent, "secondaryA", member(null, "cluster-a"), false);
        var child = new DefaultListableBeanFactory(parent);
        register(child, "primaryB", member(null, "cluster-b"), true);
        var creator = new BeanCreator(child);
        assertThat(creator.createBean("orders", (hz, name) -> hz, null, "cluster-a")).isSameAs(first);
    }

    @Test
    void matchingChildPrimaryTakesPrecedenceOverAMatchingParentPrimary() {
        var parent = new DefaultListableBeanFactory();
        register(parent, "primaryA", member(null, "cluster"), true);
        var child = new DefaultListableBeanFactory(parent);
        var local = member(null, "cluster");
        register(child, "primaryB", local, true);

        var creator = new BeanCreator(child);
        assertThat(creator.createBean("orders", (hz, name) -> hz, null, "cluster")).isSameAs(local);
    }

    @Test
    void shouldSkipAnAopProxiedClient() {
        var client = mock(HazelcastClientProxy.class);
        var dynamicConfig = mock(Config.class);
        when(dynamicConfig.getInstanceName()).thenThrow(new UnsupportedOperationException("Client configuration"));
        when(client.getConfig()).thenReturn(dynamicConfig);
        var proxyFactory = new ProxyFactory();
        proxyFactory.setInterfaces(HazelcastInstance.class);
        proxyFactory.setTarget(client);
        var proxy = (HazelcastInstance) proxyFactory.getProxy();
        assertThat(proxy).isNotInstanceOf(HazelcastClientProxy.class);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("client", HazelcastInstance.class, () -> proxy);
            context.registerBean("primaryMember", HazelcastInstance.class, () -> member("member", "cluster"),
                                 definition -> definition.setPrimary(true));
            context.register(ExportConfiguration.class);
            assertThatCode(context::refresh).doesNotThrowAnyException();
            BeanCreator creator = context.getBean(BeanCreator.class);
            assertThat(creator.createBean("orders", (hz, name) -> hz, "member", "cluster"))
                    .isSameAs(context.getBean("primaryMember"));
            verify(client, never()).getConfig();
        }
    }

    @Test
    void lazyConsumerShouldBeAbleToUseExportedObjectDuringInitialization() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("earlyConsumer", EarlyConsumer.class);
            context.register(MemberConfiguration.class);
            assertThatCode(context::refresh).doesNotThrowAnyException();
            assertThat(context.getBean(EarlyConsumer.class).size).isEqualTo(7);
        }
    }

    @Test
    void servicesCanBeUsedDuringInitializationAlongsideAnInjectedValue() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("earlyConsumer", EarlyServiceConsumer.class);
            context.register(MemberConfiguration.class);
            context.refresh();

            HazelcastInstance member = context.getBean(HazelcastInstance.class);
            verify(member.getJet()).getJobs();
            verify(member.getSql()).execute(any(SqlStatement.class));
            assertThat(context.getBean(EarlyServiceConsumer.class).label).isEqualTo("default-label");
        }
    }

    @Test
    void earlyLookupWaitsForSetterBoundConfigurationProperties() {
        try (var context = new AnnotationConfigApplicationContext()) {
            TestPropertyValues.of("export-test.instance-name=bound-member", "export-test.cluster-name=bound-cluster")
                    .applyTo(context);
            context.registerBean("earlyConsumer", EarlyConsumer.class);
            context.register(SetterBoundConfiguration.class);
            context.refresh();

            Config config = context.getBean(Config.class);
            assertThat(config.getInstanceName()).isEqualTo("bound-member");
            assertThat(config.getClusterName()).isEqualTo("bound-cluster");
            assertThat(context.getBean(EarlyConsumer.class).size).isEqualTo(7);
        }
    }

    @Test
    void earlyFieldInjectionCanResolveAMemberWithoutASeparateConfigBean() {
        HazelcastInstance member = member("member", "cluster");
        member.getConfig().addMapConfig(new MapConfig("orders"));
        IMap<Object, Object> orders = mock(IMap.class);
        when(orders.size()).thenReturn(7);
        when(member.getMap("orders")).thenReturn(orders);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("earlyConsumer", EarlyFieldConsumer.class);
            context.registerBean("member", HazelcastInstance.class, () -> member, definition -> definition.setLazyInit(true));
            context.register(ExportConfiguration.class);
            context.refresh();

            assertThat(context.getBean(EarlyFieldConsumer.class).size).isEqualTo(7);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ExposeHazelcastObjects
    static class ExportConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    @ExposeHazelcastObjects
    static class MemberConfiguration {

        @Bean
        Config config() {
            return new Config().addMapConfig(new MapConfig("orders"));
        }

        @Bean
        @Lazy
        HazelcastInstance member(Config config) {
            var hz = mock(HazelcastInstance.class);
            IMap<Object, Object> orders = mock(IMap.class);
            when(orders.size()).thenReturn(7);
            when(hz.getConfig()).thenReturn(config);
            when(hz.getMap("orders")).thenReturn(orders);
            when(hz.getJet()).thenReturn(mock(JetService.class));
            when(hz.getSql()).thenReturn(mock(SqlService.class));
            return hz;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(MutableProperties.class)
    static class SetterBoundConfiguration extends MemberConfiguration {

        @Bean
        Config config(MutableProperties properties) {
            return new Config().setInstanceName(properties.getInstanceName()).setClusterName(properties.getClusterName())
                    .addMapConfig(new MapConfig("orders"));
        }
    }

    @ConfigurationProperties(prefix = "export-test")
    public static class MutableProperties {

        private String instanceName;
        private String clusterName;

        public String getInstanceName() {
            return instanceName;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }
    }

    static class EarlyConsumer implements InitializingBean {

        private final IMap<?, ?> map;
        private int size;

        EarlyConsumer(@AutowireHz @Qualifier("orders") IMap<?, ?> map) {
            this.map = map;
        }

        @Override
        public void afterPropertiesSet() {
            size = map.size();
        }
    }

    static class EarlyFieldConsumer implements InitializingBean {

        @Autowired
        @Lazy
        @Qualifier("orders")
        private IMap<?, ?> map;
        private int size;

        @Override
        public void afterPropertiesSet() {
            size = map.size();
        }
    }

    static class EarlyServiceConsumer implements InitializingBean {

        @AutowireHz
        private JetService jet;
        @AutowireHz
        private SqlService sql;
        @Value("${export-label:default-label}")
        private String label;

        @Override
        public void afterPropertiesSet() {
            jet.getJobs();
            sql.execute(new SqlStatement("SELECT 1"));
        }
    }

    private static HazelcastInstance member(String instanceName, String clusterName) {
        var member = mock(HazelcastInstance.class);
        when(member.getConfig()).thenReturn(new Config().setInstanceName(instanceName).setClusterName(clusterName));
        return member;
    }

    private static void register(DefaultListableBeanFactory factory, String name, HazelcastInstance instance, boolean primary) {
        var definition = new RootBeanDefinition(HazelcastInstance.class, () -> instance);
        definition.setPrimary(primary);
        factory.registerBeanDefinition(name, definition);
    }
}
