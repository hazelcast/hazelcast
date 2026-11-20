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
import com.hazelcast.spring.ExposeHazelcastObjects.Configuration;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.impl.spi.VectorCollectionLocator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * Registers lazy exported definitions after a {@link Config} bean has been initialized, including property binding.
 * An initialized {@link HazelcastInstance member} also supplies its configuration, supporting factories that do not expose
 * a separate Config bean.
 * Clients, including Spring AOP proxies around them, are excluded because they do not provide member configuration.
 */
final class BeanExporterPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private static final ILogger LOGGER = Logger.getLogger(BeanExporterPostProcessor.class);
    private final Set<Class<?>> registeredConfigClasses;
    private final ObjectProvider<Configuration> exportConfiguration;
    private ApplicationContext applicationContext;

    BeanExporterPostProcessor(Set<Class<?>> registeredConfigClasses,
                              ObjectProvider<Configuration> exportConfiguration) {
        this.registeredConfigClasses = registeredConfigClasses;
        this.exportConfiguration = exportConfiguration;
    }

    @Nonnull
    @Override
    public Object postProcessAfterInitialization(@Nonnull Object bean, @Nonnull String beanName) throws BeansException {
        if (bean instanceof Config hazelcastConfig) {
            configure(hazelcastConfig);
        } else if (bean instanceof HazelcastInstance hazelcastInstance
            && !BeanCreator.isClient(hazelcastInstance)) {
            configure(hazelcastInstance.getConfig());
        }
        return bean;

    }

    private void configure(Config hazelcastConfiguration) {
        final Configuration exposeConf = exportConfiguration.getIfAvailable(Configuration::empty);
        AutowireCapableBeanFactory autowireCapableBeanFactory = applicationContext.getAutowireCapableBeanFactory();
        assert autowireCapableBeanFactory instanceof BeanDefinitionRegistry;
        BeanDefinitionRegistry beanRegistry = (BeanDefinitionRegistry) autowireCapableBeanFactory;
        if (LOGGER.isFineEnabled()) {
            LOGGER.fine("Exposing Hazelcast objects, using configuration: %s", exposeConf);
        }

        var reg = new BeanExporterPostProcessor.RegistrationChain(beanRegistry, exposeConf, registeredConfigClasses,
                                                                  hazelcastConfiguration.getInstanceName(),
                                                                  hazelcastConfiguration.getClusterName())
                .register(beanRegistry, "jetService", JetService.class, (hz, n) -> hz.getJet())
                .register(beanRegistry, "sqlService", SqlService.class, (hz, n) -> hz.getSql())

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
        if (vectorCollectionPresent()) {
            registerVectorCollections(reg, hazelcastConfiguration);
        }
    }

    private boolean vectorCollectionPresent() {
        if (VectorCollectionLocator.isAvailable()) {
            return true;
        }
        LOGGER.fine("Hazelcast vector module is not found, skipping adding VectorCollection beans");
        return false;
    }

    private void registerVectorCollections(RegistrationChain reg,
                                           Config hazelcastConfiguration) {
        reg.register(hazelcastConfiguration.getVectorCollectionConfigs(), VectorCollection.class,
                     (hz, name) -> VectorCollectionLocator.get().getVectorCollection(hz, name));
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

    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    record RegistrationChain(BeanDefinitionRegistry registry,
                             Configuration configuration,
                             Set<Class<?>> registeredConfigClasses,
                             String instanceName,
                             String clusterName) {

        RegistrationChain register(final Map<String, ?> conf,
                                   final Class<?> clazz,
                                   final RetrieveDistributedObjectFunction retrieveDistributedObjectFunction) {
            for (Map.Entry<String, ?> entry : conf.entrySet()) {
                register(registry, entry.getKey(), clazz, retrieveDistributedObjectFunction);
            }
            registeredConfigClasses.add(clazz);
            return this;
        }

        /**
         * @see BeanCreator#createBean
         */
        RegistrationChain register(final BeanDefinitionRegistry registry,
                                   final String name,
                                   final Class<?> clazz,
                                   final RetrieveDistributedObjectFunction retrieveDistributedObjectFunction) {
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
                RootBeanDefinition beanDefinition = new RootBeanDefinition();
                beanDefinition.setTargetType(clazz);
                beanDefinition.setLazyInit(true);
                beanDefinition.addQualifier(new AutowireCandidateQualifier(Qualifier.class, name));

                beanDefinition.setFactoryBeanName("hzInternalBeanCreator");
                beanDefinition.setFactoryMethodName("createBean");
                ConstructorArgumentValues constructorArgumentValues = new ConstructorArgumentValues();
                constructorArgumentValues.addIndexedArgumentValue(0, name);
                constructorArgumentValues.addIndexedArgumentValue(1, retrieveDistributedObjectFunction);
                constructorArgumentValues.addIndexedArgumentValue(2, instanceName);
                constructorArgumentValues.addIndexedArgumentValue(3, clusterName);
                beanDefinition.setConstructorArgumentValues(constructorArgumentValues);
                if (DistributedObject.class.isAssignableFrom(clazz)) {
                    beanDefinition.setDestroyMethodName("destroy");
                }
                registry.registerBeanDefinition(name, beanDefinition);
            }
            registeredConfigClasses.add(clazz);
            return this;
        }
    }
}
