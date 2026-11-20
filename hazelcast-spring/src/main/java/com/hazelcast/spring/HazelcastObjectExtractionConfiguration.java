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

import com.hazelcast.spi.annotation.PrivateApi;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.springframework.beans.factory.config.BeanDefinition.ROLE_SUPPORT;

/**
 * Configuration used by {@link ExposeHazelcastObjects} to dynamically register all Hazelcast objects as Spring beans.
 *
 * <p>Initialization proceeds in three stages:
 * <ol>
 *     <li>The static factory post-processor installs {@link HazelcastLazyDependencyResolver}, without obtaining any
 *     configuration or member beans. Bean post-processors, including property binding, are not yet installed.</li>
 *     <li>During normal bean initialization, {@link BeanExporterPostProcessor} reads an initialized configuration or
 *     member and registers lazy object definitions. This allows configuration properties to be bound first.</li>
 *     <li>On first use, an exported definition invokes {@link BeanCreator} to select a member and retrieve the object.
 *     If a consumer uses its lazy dependency before stage two has run, the resolver initializes configuration/member
 *     beans through Spring's normal lifecycle and retries the lookup.</li>
 * </ol>
 *
 * @since 5.6
 */
@Import(HazelcastExposeObjectRegistrar.class)
@Configuration
@PrivateApi
public class HazelcastObjectExtractionConfiguration {

    private static final Set<Class<?>> REGISTERED_CONFIG_CLASSES = ConcurrentHashMap.newKeySet();
    @Bean
    @Role(ROLE_SUPPORT)
    static BeanExporterPostProcessor hzInternalBeanExporter(
        ObjectProvider<ExposeHazelcastObjects.Configuration> exposeHazelcastObjectConfiguration) {
        return new BeanExporterPostProcessor(REGISTERED_CONFIG_CLASSES, exposeHazelcastObjectConfiguration);
    }

    @Bean
    @Role(ROLE_SUPPORT)
    static BeanFactoryPostProcessor hzInternalLazyDependencyResolver() {
        return beanFactory -> {
            if (beanFactory instanceof DefaultListableBeanFactory factory) {
                factory.setAutowireCandidateResolver(new HazelcastLazyDependencyResolver(factory.getAutowireCandidateResolver()));
            }
        };
    }

    /**
     * The exported definitions refer to this bean by name. If the bean name changes, update
     * {@link BeanExporterPostProcessor.RegistrationChain#register} as well.
     */
    @Bean
    @Role(ROLE_SUPPORT)
    static BeanCreator hzInternalBeanCreator(ConfigurableListableBeanFactory beanFactory) {
        return new BeanCreator(beanFactory);
    }

    @Nonnull
    public Set<Class<?>> registeredTypesOfBeans() {
        return Set.copyOf(REGISTERED_CONFIG_CLASSES);
    }
}
