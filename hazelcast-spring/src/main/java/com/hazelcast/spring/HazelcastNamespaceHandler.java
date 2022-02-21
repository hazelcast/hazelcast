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

package com.hazelcast.spring;

import com.hazelcast.spring.hibernate.RegionFactoryBeanDefinitionParser;
import com.hazelcast.spring.jet.JetBeanDefinitionParser;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

import java.util.HashSet;
import java.util.Set;

/**
 * Hazelcast Custom Namespace Definitions.
 */
public class HazelcastNamespaceHandler extends NamespaceHandlerSupport {

    static final Set<String> CP_TYPES = new HashSet<>();
    private static final String MAP = "map";
    private static final String MULTI_MAP = "multiMap";
    private static final String REPLICATED_MAP = "replicatedMap";
    private static final String QUEUE = "queue";
    private static final String TOPIC = "topic";
    private static final String SET = "set";
    private static final String LIST = "list";
    private static final String EXECUTOR_SERVICE = "executorService";
    private static final String DURABLE_EXECUTOR_SERVICE = "durableExecutorService";
    private static final String SCHEDULED_EXECUTOR_SERVICE = "scheduledExecutorService";
    private static final String RINGBUFFER = "ringbuffer";
    private static final String CARDINALITY_ESTIMATOR = "cardinalityEstimator";
    private static final String FLAKE_ID_GENERATOR = "flakeIdGenerator";
    private static final String ATOMIC_LONG = "atomicLong";
    private static final String ATOMIC_REFERENCE = "atomicReference";
    private static final String COUNT_DOWN_LATCH = "countDownLatch";
    private static final String SEMAPHORE = "semaphore";
    private static final String LOCK = "lock";
    private static final String RELIABLE_TOPIC = "reliableTopic";
    private static final String PNCOUNTER = "PNCounter";

    static {
        CP_TYPES.add(LOCK);
        CP_TYPES.add(SEMAPHORE);
        CP_TYPES.add(COUNT_DOWN_LATCH);
        CP_TYPES.add(ATOMIC_LONG);
        CP_TYPES.add(ATOMIC_REFERENCE);
    }

    @Override
    public void init() {
        registerBeanDefinitionParser("config", new HazelcastConfigBeanDefinitionParser());
        registerBeanDefinitionParser("hazelcast", new HazelcastInstanceDefinitionParser());
        registerBeanDefinitionParser("client", new HazelcastClientBeanDefinitionParser());
        registerBeanDefinitionParser("client-failover",
                new HazelcastFailoverClientBeanDefinitionParser());
        registerBeanDefinitionParser("hibernate-region-factory", new RegionFactoryBeanDefinitionParser());
        registerBeanDefinitionParser("cache-manager", new CacheManagerBeanDefinitionParser());
        registerBeanDefinitionParser("jet", new JetBeanDefinitionParser());
        String[] types =
                {MAP, MULTI_MAP, REPLICATED_MAP, QUEUE, TOPIC, SET, LIST, EXECUTOR_SERVICE,
                 DURABLE_EXECUTOR_SERVICE, SCHEDULED_EXECUTOR_SERVICE, RINGBUFFER, CARDINALITY_ESTIMATOR,
                 FLAKE_ID_GENERATOR, ATOMIC_LONG, ATOMIC_REFERENCE, COUNT_DOWN_LATCH, SEMAPHORE,
                 LOCK, RELIABLE_TOPIC, PNCOUNTER};
        for (String type : types) {
            registerBeanDefinitionParser(type, new HazelcastTypeBeanDefinitionParser(type));
        }
    }
}
