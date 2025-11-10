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
package com.hazelcast.spring;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test addresses tip given <a href="https://docs.hazelcast.com/hazelcast/5.5/spring/best-practices">here</a>.
 */
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(classes = TestSpringNotCallsEntrySet.Conf.class)
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
public class TestSpringNotCallsEntrySet {

    @Autowired
    @Qualifier("mapBean")
    private Map<String, String> myMap;

    @Autowired
    @Qualifier("dynamicMap")
    private Map<String, String> dynamicMap;

    @Autowired
    @Qualifier("listBean")
    private List<String> myList;

    @Autowired
    @Qualifier("stringListBean")
    private List<String> stringListBean;

    @Test
    void entrySetNotCalled() {
        assertNotNull(myMap);
        assertInstanceOf(MyMap.class, myMap);
        assertInstanceOf(MyMap.class, dynamicMap);
        assertFalse(((MyMap<String, String>) myMap).wasIterationCalled());
        assertFalse(((MyMap<String, String>) dynamicMap).wasIterationCalled());

        assertNotNull(myList);
        assertInstanceOf(MyList.class, myList);
        assertInstanceOf(MyList.class, stringListBean);
        assertFalse(((MyList<String>) myList).wasIterationCalled());
        assertFalse(((MyList<String>) stringListBean).wasIterationCalled());
    }

    @Configuration(proxyBeanMethods = false)
    public static class Conf {
        // generic types not set on purpose, so that Spring cannot deduce it in any way
        @Bean
        @SuppressWarnings("rawtypes")
        public Map mapBean() {
            return new MyMap();
        }

        // generic types not set on purpose, so that Spring cannot deduce it in any way
        @Bean
        @SuppressWarnings("rawtypes")
        public List listBean() {
            return new MyList();
        }

        @Bean
        public List<String> stringListBean() {
            return new MyList<>();
        }

        @Bean
        @Nonnull
        public static BeanDefinitionRegistryPostProcessor beanExposer() {
            return new Exporter();
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    private static class Exporter implements BeanDefinitionRegistryPostProcessor {
        @Override
        @SuppressWarnings("rawtypes")
        public void postProcessBeanDefinitionRegistry(@Nonnull BeanDefinitionRegistry registry) {
            var builder = BeanDefinitionBuilder.rootBeanDefinition(Map.class, () -> new MyMap())
                                               .setLazyInit(true);
            registry.registerBeanDefinition("dynamicMap", builder.getBeanDefinition());
        }

        @Override
        public void postProcessBeanFactory(@Nonnull ConfigurableListableBeanFactory configurableListableBeanFactory)
                throws BeansException {
        }
    }

    public static class MyMap<K, V> extends HashMap<K, V> {
        private volatile boolean called = false;

        boolean wasIterationCalled() {
            return called;
        }

        @Override
        @Nonnull
        public Set<Entry<K, V>> entrySet() {
            called = true;
            return super.entrySet();
        }

        @Override
        @Nonnull
        public Collection<V> values() {
            called = true;
            return super.values();
        }

        @Override
        @Nonnull
        public Set<K> keySet() {
            called = true;
            return super.keySet();
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            called = true;
            super.forEach(action);
        }
    }

    public static class MyList<T> extends ArrayList<T> {

        private volatile boolean called = false;

        boolean wasIterationCalled() {
            return called;
        }

        @Override
        @Nonnull
        public Iterator<T> iterator() {
            called = true;
            return super.iterator();
        }

        @Override
        public void forEach(Consumer<? super T> action) {
            called = true;
            super.forEach(action);
        }

        @Override
        @Nonnull
        public Object[] toArray() {
            called = true;
            return super.toArray();
        }

        @Override
        @Nonnull
        public <T1> T1[] toArray(T1[] a) {
            called = true;
            return super.toArray(a);
        }

        @Override
        @Nonnull
        public Stream<T> stream() {
            called = true;
            return super.stream();
        }

        @Override
        @Nonnull
        public Stream<T> parallelStream() {
            called = true;
            return super.parallelStream();
        }
    }
}
