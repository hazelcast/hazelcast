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

import com.hazelcast.cache.ICache;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

import javax.annotation.Nonnull;

final class BeanExporter
        implements BeanDefinitionRegistryPostProcessor {

    private static final ILogger LOGGER = Logger.getLogger(BeanExporter.class);
    private final HazelcastInstance hazelcastInstance;
    private final ExposeHazelcastObjects.Configuration exposeConf;

    BeanExporter(HazelcastInstance hazelcastInstance, ExposeHazelcastObjects.Configuration exposeConf) {
        this.hazelcastInstance = hazelcastInstance;
        this.exposeConf = exposeConf;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(@Nonnull BeanDefinitionRegistry registry)
            throws BeansException {

        final Config hazelcastConfiguration = hazelcastInstance.getConfig();

        if (LOGGER.isFineEnabled()) {
            LOGGER.fine("Exposing Hazelcast objects, using configuration: %s", exposeConf);
        }

        var reg = new HazelcastObjectExtractionConfiguration.RegistrationChain(hazelcastInstance, registry, exposeConf)
                .register(registry, "jetService", JetService.class, (hz, n) -> hz.getJet())
                .register(registry, "sqlService", SqlService.class, (hz, n) -> hz.getSql())

                .register(hazelcastConfiguration.getMapConfigs(), IMap.class, HazelcastInstance::getMap)
                .register(hazelcastConfiguration.getQueueConfigs(), IQueue.class, HazelcastInstance::getQueue)
                .register(hazelcastConfiguration.getFlakeIdGeneratorConfigs(), FlakeIdGenerator.class,
                          HazelcastInstance::getFlakeIdGenerator)
                .register(hazelcastConfiguration.getMultiMapConfigs(), MultiMap.class, HazelcastInstance::getMultiMap)
                .register(hazelcastConfiguration.getPNCounterConfigs(), PNCounter.class, HazelcastInstance::getPNCounter)
                .register(hazelcastConfiguration.getReliableTopicConfigs(), ITopic.class, HazelcastInstance::getReliableTopic)
                .register(hazelcastConfiguration.getReplicatedMapConfigs(), ReplicatedMap.class,
                          HazelcastInstance::getReplicatedMap)
                .register(hazelcastConfiguration.getRingbufferConfigs(), Ringbuffer.class, HazelcastInstance::getRingbuffer)
                .register(hazelcastConfiguration.getSetConfigs(), ISet.class, HazelcastInstance::getSet)
                .register(hazelcastConfiguration.getTopicConfigs(), ITopic.class, HazelcastInstance::getTopic)
                .register(hazelcastConfiguration.getScheduledExecutorConfigs(), IScheduledExecutorService.class,
                          HazelcastInstance::getScheduledExecutorService)
                .register(hazelcastConfiguration.getDurableExecutorConfigs(), DurableExecutorService.class,
                          HazelcastInstance::getDurableExecutorService)
                .register(hazelcastConfiguration.getExecutorConfigs(), IExecutorService.class,
                          HazelcastInstance::getExecutorService)
                .register(hazelcastConfiguration.getListConfigs(), IList.class,
                          HazelcastInstance::getList)
                .register(hazelcastConfiguration.getCardinalityEstimatorConfigs(), CardinalityEstimator.class,
                          HazelcastInstance::getCardinalityEstimator)

                .register(hazelcastConfiguration.getCPSubsystemConfig().getCpMapConfigs(), CPMap.class,
                          (i, n) -> i.getCPSubsystem().getMap(n))
                .register(hazelcastConfiguration.getCPSubsystemConfig().getLockConfigs(), FencedLock.class,
                          (i, n) -> i.getCPSubsystem().getLock(n))
                .register(hazelcastConfiguration.getCPSubsystemConfig().getSemaphoreConfigs(), ISemaphore.class,
                          (i, n) -> i.getCPSubsystem().getSemaphore(n));

        if (cachePresent()) {
            reg.register(hazelcastConfiguration.getCacheConfigs(), ICache.class,
                         (instance, name) -> instance.getCacheManager().getCache(name));
        }
    }

    private boolean cachePresent() {
        final String cacheClassName = "javax.cache.Cache";
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Class<?> aClass = contextClassLoader.loadClass(cacheClassName);
            assert aClass != null;
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            LOGGER.fine("Javax Cache class '%s' not found, skipping adding ICache beans", cacheClassName);
            return false;
        }
    }
}
