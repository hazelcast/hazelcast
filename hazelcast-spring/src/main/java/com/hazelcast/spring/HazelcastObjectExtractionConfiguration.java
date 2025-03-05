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
import com.hazelcast.core.DistributedObject;
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
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Configuration used by {@link ExposeHazelcastObjects} to dynamically register all Hazelcast objects as Spring beans.
 * @since 6.0
 */
@Configuration(proxyBeanMethods = false)
@AutoConfiguration
@AutoConfigureAfter(HazelcastAutoConfiguration.class)
public class HazelcastObjectExtractionConfiguration {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastObjectExtractionConfiguration.class);

    private static Set<Class<?>> registeredConfigClasses = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @ConditionalOnMissingBean(ExposeHazelcastObjects.Configuration.class)
    public static class ExposeConf {
        @Bean
        public ExposeHazelcastObjects.Configuration hzInternalBeanInfo() {
            return ExposeHazelcastObjects.Configuration.empty();
        }
    }

    @Bean
    @Nonnull
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public static BeanDefinitionRegistryPostProcessor hzInternalBeanExposer(
            @Nonnull HazelcastInstance hazelcastInstance,
            @Nonnull ExposeHazelcastObjects.Configuration exposeConf) {
        return new Exporter(hazelcastInstance, exposeConf);
    }

    @Nonnull
    public Set<Class<?>> registeredTypesOfBeans() {
        return Set.copyOf(registeredConfigClasses);
    }

    private record Exporter(HazelcastInstance hazelcastInstance, ExposeHazelcastObjects.Configuration exposeConf)
            implements BeanDefinitionRegistryPostProcessor {

        @Override
        public void postProcessBeanDefinitionRegistry(@Nonnull BeanDefinitionRegistry registry)
                throws BeansException {

            final Config hazelcastConfiguration = hazelcastInstance.getConfig();

            new Registration(hazelcastInstance, registry, exposeConf)
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
                    .register(hazelcastConfiguration.getCacheConfigs(), ICache.class,
                              (i, n) -> i.getCacheManager().getCache(n))
                    .register(hazelcastConfiguration.getCardinalityEstimatorConfigs(), CardinalityEstimator.class,
                              HazelcastInstance::getCardinalityEstimator)

                    .register(hazelcastConfiguration.getCPSubsystemConfig().getCpMapConfigs(), CPMap.class,
                              (i, n) -> i.getCPSubsystem().getMap(n))
                    .register(hazelcastConfiguration.getCPSubsystemConfig().getLockConfigs(), FencedLock.class,
                              (i, n) -> i.getCPSubsystem().getLock(n))
                    .register(hazelcastConfiguration.getCPSubsystemConfig().getSemaphoreConfigs(), ISemaphore.class,
                              (i, n) -> i.getCPSubsystem().getSemaphore(n))
            ;
        }
    }

    private record Registration (HazelcastInstance hazelcastInstance,
                                 BeanDefinitionRegistry registry,
                                 ExposeHazelcastObjects.Configuration configuration) {

        <T> Registration register(final Map<String, ?> conf,
                                                           final Class<T> clazz,
                                                           final BiFunction<HazelcastInstance, String, T> supplierProvider) {
            for (Map.Entry<String, ?> entry : conf.entrySet()) {
                register(registry, entry.getKey(), clazz, supplierProvider);
            }
            registeredConfigClasses.add(clazz);
            return this;
        }

        <T> Registration register(final BeanDefinitionRegistry registry,
                                                           final String name,
                                                           final Class<T> clazz,
                                                           final BiFunction<HazelcastInstance, String, T> supplierProvider) {
            boolean containsBeanDefinition = registry.containsBeanDefinition(name);
            boolean canIncludeByName = configuration.canInclude(name);
            boolean canIncludeByType = configuration.canInclude(clazz);
            if (LOGGER.isFineEnabled()) {
                if (containsBeanDefinition) {
                    LOGGER.fine("Not registering bean %s of type %s, there is already bean with this name",
                                name, clazz.getSimpleName());
                }
                if (!canIncludeByName) {
                    LOGGER.fine("Not registering bean %s of type %s, this name was excluded in the configuration",
                                name, clazz.getSimpleName());
                }
                if (!canIncludeByType) {
                    LOGGER.fine("Not registering bean %s of type %s, this type of objects was excluded in the configuration",
                                name, clazz.getSimpleName());
                }
            }
            if (!containsBeanDefinition && canIncludeByName && canIncludeByType) {
                final Supplier<T> supplier = () -> supplierProvider.apply(hazelcastInstance, name);
                var builder = BeanDefinitionBuilder.rootBeanDefinition(clazz, supplier)
                                                   .setLazyInit(true);
                if (DistributedObject.class.isAssignableFrom(clazz)) {
                    builder.setDestroyMethodName("destroy");
                }
                registry.registerBeanDefinition(name, builder.getBeanDefinition());
            }
            registeredConfigClasses.add(clazz);
            return this;
        }
    }
}
