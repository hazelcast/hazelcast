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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObjectExportLifecycleTest {

    @Test
    void defersMemberAndObjectCreationUntilTheExportedBeanIsUsed() {
        try (var context = new AnnotationConfigApplicationContext(DeferredMemberConfiguration.class)) {
            var configuration = context.getBean(DeferredMemberConfiguration.class);
            assertThat(context.containsBean("orders")).isTrue();
            assertThat(context.getBeanFactory().containsSingleton("member")).isFalse();
            assertThat(context.getBeanFactory().containsSingleton("orders")).isFalse();
            verify(configuration.member, never()).getMap(anyString());

            assertThat(context.getBean("orders")).isSameAs(configuration.orders);
            assertThat(context.getBeanFactory().containsSingleton("member")).isTrue();
            assertThat(context.getBean("orders")).isSameAs(configuration.orders);
            verify(configuration.member).getMap("orders");
        }
    }

    @Test
    void destroysCreatedObjectsWithoutInitializingUnusedExportsOnContextClose() {
        DeferredMemberConfiguration configuration;
        try (var context = new AnnotationConfigApplicationContext(DeferredMemberConfiguration.class)) {
            configuration = context.getBean(DeferredMemberConfiguration.class);
            context.getBean("orders");
            verify(configuration.orders, never()).destroy();
        }

        verify(configuration.orders).destroy();
        verify(configuration.returns, never()).destroy();
        verify(configuration.member, never()).getMap("returns");
    }

    @Test
    void preservesAnExistingUserBeanWithTheSameName() {
        IMap<?, ?> userMap = mock(IMap.class);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("orders", IMap.class, () -> userMap);
            context.register(DeferredMemberConfiguration.class);
            context.refresh();

            var configuration = context.getBean(DeferredMemberConfiguration.class);
            assertThat(context.getBean("orders")).isSameAs(userMap);
            assertThat(context.getBeanFactory().containsSingleton("member")).isFalse();
            verify(configuration.member, never()).getMap("orders");
        }
    }

    @Test
    void injectsQualifiedLazyObjectsIntoAConsumerCreatedBeforeTheirRegistration() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("consumer", EarlyConsumer.class);
            context.register(DeferredMemberConfiguration.class);
            context.refresh();

            var configuration = context.getBean(DeferredMemberConfiguration.class);
            EarlyConsumer consumer = context.getBean(EarlyConsumer.class);
            assertThat(context.getBeanFactory().containsSingleton("member")).isFalse();
            assertThat(consumer.constructorMap.getName()).isEqualTo("orders");
            assertThat(consumer.fieldMap.getName()).isEqualTo("returns");
            verify(configuration.member).getMap("orders");
            verify(configuration.member).getMap("returns");
        }
    }

    static class EarlyConsumer {

        private final IMap<String, String> constructorMap;

        @AutowireHz
        @Qualifier("returns")
        private IMap<String, String> fieldMap;

        EarlyConsumer(@AutowireHz @Qualifier("orders") IMap<String, String> constructorMap) {
            this.constructorMap = constructorMap;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ExposeHazelcastObjects
    static class DeferredMemberConfiguration {

        private final HazelcastInstance member = mock(HazelcastInstance.class);
        private final IMap<String, String> orders = map("orders");
        private final IMap<String, String> returns = map("returns");

        @Bean
        Config config() {
            Config config = new Config()
                    .addMapConfig(new MapConfig("orders"))
                    .addMapConfig(new MapConfig("returns"));
            when(member.getConfig()).thenReturn(config);
            when(member.<String, String>getMap("orders")).thenReturn(orders);
            when(member.<String, String>getMap("returns")).thenReturn(returns);
            return config;
        }

        @Bean
        @Lazy
        HazelcastInstance member(Config config) {
            return member;
        }

        @SuppressWarnings("unchecked")
        private static IMap<String, String> map(String name) {
            IMap<String, String> map = mock(IMap.class);
            when(map.getName()).thenReturn(name);
            return map;
        }
    }
}
