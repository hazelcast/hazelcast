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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

import javax.annotation.Nonnull;

/**
 * Configuration used by {@link ExposeHazelcastObjects} to dynamically register its configuration.
 * @since 5.6
 */
public final class HazelcastExposeObjectRegistrar implements ImportBeanDefinitionRegistrar {

    private final ILogger logger = Logger.getLogger(HazelcastExposeObjectRegistrar.class);
    private final BeanFactory beanFactory;
    public HazelcastExposeObjectRegistrar(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public void registerBeanDefinitions(@Nonnull AnnotationMetadata importingClassMetadata,
                                        @Nonnull BeanDefinitionRegistry registry) {
        final var annotatedConfiguration = ExposeHazelcastObjects.Configuration.toConfiguration(importingClassMetadata);
        if (logger.isFineEnabled()) {
            logger.fine("Registering additional ExposeHazelcastObjects configuration: %s", annotatedConfiguration);
        }

        ExposeHazelcastObjects.Configuration hzBeanExportConf = ExposeHazelcastObjects.Configuration.empty();

        String hzBeanExportConfName = "hzBeanExportConf";
        if (registry.containsBeanDefinition(hzBeanExportConfName)) {
            hzBeanExportConf = beanFactory.getBean(hzBeanExportConfName, ExposeHazelcastObjects.Configuration.class);
        } else {
            final var toInsert = hzBeanExportConf;
            registry.registerBeanDefinition(hzBeanExportConfName, BeanDefinitionBuilder.rootBeanDefinition(
                    ExposeHazelcastObjects.Configuration.class, () -> toInsert).getBeanDefinition());
        }

        hzBeanExportConf.includeOther(annotatedConfiguration);
    }
}
