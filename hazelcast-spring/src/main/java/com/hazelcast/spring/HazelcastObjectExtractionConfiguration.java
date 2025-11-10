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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Configuration used by {@link ExposeHazelcastObjects} to dynamically register all Hazelcast objects as Spring beans.
 * @since 5.6
 */
@Configuration(proxyBeanMethods = false)
@AutoConfiguration
@AutoConfigureAfter({HazelcastAutoConfiguration.class, HazelcastExposeObjectRegistrar.class})
public class HazelcastObjectExtractionConfiguration {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastObjectExtractionConfiguration.class);

    private static final Set<Class<?>> REGISTERED_CONFIG_CLASSES = ConcurrentHashMap.newKeySet();

    @Bean
    @Nonnull
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public static BeanDefinitionRegistryPostProcessor hzInternalBeanExposer(@Nonnull HazelcastInstance hazelcastInstance,
                                                                            ApplicationContext applicationContext) {
        ExposeHazelcastObjects.Configuration hzBeanExportConf = ExposeHazelcastObjects.Configuration.empty();
        if (applicationContext.containsBean("hzBeanExportConf")) {
            hzBeanExportConf = applicationContext.getBean("hzBeanExportConf", ExposeHazelcastObjects.Configuration.class);
        }
        return new BeanExporter(hazelcastInstance, hzBeanExportConf);
    }

    @Nonnull
    public Set<Class<?>> registeredTypesOfBeans() {
        return Set.copyOf(REGISTERED_CONFIG_CLASSES);
    }

    record RegistrationChain(HazelcastInstance hazelcastInstance,
                             BeanDefinitionRegistry registry,
                             ExposeHazelcastObjects.Configuration configuration) {

        RegistrationChain register(final Map<String, ?> conf,
                                   final Class<?> clazz,
                                   final BiFunction<HazelcastInstance, String, Object> supplierProvider) {
            for (Map.Entry<String, ?> entry : conf.entrySet()) {
                register(registry, entry.getKey(), clazz, supplierProvider);
            }
            REGISTERED_CONFIG_CLASSES.add(clazz);
            return this;
        }

        RegistrationChain register(final BeanDefinitionRegistry registry,
                                   final String name,
                                   final Class<?> clazz,
                                   final BiFunction<HazelcastInstance, String, Object> supplierProvider) {
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
                final Supplier<Object> supplier = () -> supplierProvider.apply(hazelcastInstance, name);
                RootBeanDefinition beanDefinition = new RootBeanDefinition();
                beanDefinition.setTargetType(clazz);
                beanDefinition.setInstanceSupplier(supplier);
                beanDefinition.setLazyInit(true);
                if (DistributedObject.class.isAssignableFrom(clazz)) {
                    beanDefinition.setDestroyMethodName("destroy");
                }
                registry.registerBeanDefinition(name, beanDefinition);
            }
            REGISTERED_CONFIG_CLASSES.add(clazz);
            return this;
        }
    }
}
