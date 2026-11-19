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
import com.hazelcast.core.HazelcastInstance;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Resolves the member for a lazily exported bean, then retrieves the object using its factory function.
 * Definitions registered by {@link BeanExporterPostProcessor} carry the source configuration's instance and cluster names.
 *
 * <p>An explicit instance name selects that member directly. Otherwise, matching primary members are considered in the
 * current bean factory before its ancestors. If no primary matches, exactly one matching member must exist across the
 * hierarchy. A primary from another cluster cannot influence selection, and clients are never candidates.
 *
 * <p>Names and bean-definition metadata are inspected before creating beans. Existing singletons are checked first,
 * so a named match can avoid starting unrelated lazy beans. For unnamed configurations, primary selection avoids creating
 * non-primary beans when possible. A factory declared only as {@link HazelcastInstance} may still require initialization
 * to determine its cluster or whether it creates a client.
 *
 * @since 6.0
 */
class BeanCreator {

    private final ConfigurableListableBeanFactory beanFactory;

    BeanCreator(ConfigurableListableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * Used reflectively by Spring. If the method name or parameter order changes, update
     * {@link BeanExporterPostProcessor.RegistrationChain#register} as well.
     */
    @SuppressWarnings("unused")
    Object createBean(@Nonnull final String beanName,
                      @Nonnull final RetrieveDistributedObjectFunction retrieveDistributedObjectFunction,
                      @Nullable String instanceName,
                      @Nullable String clusterName) {
        String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory, HazelcastInstance.class, true, false);
        Set<HazelcastInstance> matches;
        if (instanceName != null) {
            matches = matchingInstances(names, instanceName, clusterName, false, null);
        } else {
            matches = primaryInstances(names, clusterName);
            if (matches.isEmpty()) {
                matches = matchingInstances(names, null, clusterName, false, null);
            }
        }
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("No instances match criteria: instanceName=" + instanceName
                                                       + ", clusterName=" + clusterName);
        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("More than one instance matches criteria: instanceName=" + instanceName
                                                       + ", clusterName=" + clusterName);
        }
        return retrieveDistributedObjectFunction.retrieve(matches.iterator().next(), beanName);
    }

    /**
     * Finds matching primary members in the nearest bean factory that declares any.
     * The candidate names include beans visible from ancestor factories, so collecting all primaries at once would
     * incorrectly make a matching child primary ambiguous with a matching parent primary. Instead, each iteration
     * considers only candidates owned by that factory; a matching local primary takes precedence over ancestor primaries.
     */
    private Set<HazelcastInstance> primaryInstances(String[] names, String clusterName) {
        ConfigurableListableBeanFactory scope = beanFactory;
        while (scope != null) {
            Set<HazelcastInstance> matches = matchingInstances(names, null, clusterName, true, scope);
            if (!matches.isEmpty()) {
                return matches;
            }
            scope = scope.getParentBeanFactory() instanceof ConfigurableListableBeanFactory parent ? parent : null;
        }
        return Set.of();
    }

    /**
     * Collects eligible members, optionally restricting selection to primaries in one factory.
     */
    private Set<HazelcastInstance> matchingInstances(String[] names, String instanceName, String clusterName,
                                                     boolean primaryOnly, @Nullable ConfigurableListableBeanFactory scope) {
        Set<HazelcastInstance> matches = new LinkedHashSet<>();
        for (boolean initialized : new boolean[]{true, false}) {
            for (String name : names) {
                ConfigurableListableBeanFactory owner = owningFactory(beanFactory, name);
                if (!isCandidate(owner, name, primaryOnly, initialized, scope)) {
                    continue;
                }
                Class<?> type = owner.getType(name, false);
                if (type != null && HazelcastClientProxy.class.isAssignableFrom(type)) {
                    continue;
                }
                HazelcastInstance instance = owner.getBean(name, HazelcastInstance.class);
                if (isClient(instance)) {
                    continue;
                }
                Config config = instance.getConfig();
                if (matches(instanceName, config.getInstanceName()) && matches(clusterName, config.getClusterName())) {
                    matches.add(instance);
                    if (instanceName != null) {
                        return matches;
                    }
                }
            }
        }
        return matches;
    }

    private static boolean isCandidate(ConfigurableListableBeanFactory factory, String name,
                                       boolean primaryOnly, boolean initialized,
                                       @Nullable ConfigurableListableBeanFactory scope) {
        if (scope != null && factory != scope || factory.containsSingleton(name) != initialized
                || factory.isCurrentlyInCreation(name)) {
            return false;
        }
        if (!factory.containsBeanDefinition(name)) {
            return !primaryOnly;
        }
        BeanDefinition definition = factory.getMergedBeanDefinition(name);
        return definition.isAutowireCandidate() && (!primaryOnly || definition.isPrimary());
    }

    /** Identifies clients through Spring AOP wrappers without replacing the proxied bean. */
    static boolean isClient(HazelcastInstance instance) {
        return HazelcastClientProxy.class.isAssignableFrom(AopProxyUtils.ultimateTargetClass(instance));
    }

    static ConfigurableListableBeanFactory owningFactory(ConfigurableListableBeanFactory factory, String name) {
        ConfigurableListableBeanFactory owner = factory;
        while (!owner.containsLocalBean(name)
                && owner.getParentBeanFactory() instanceof ConfigurableListableBeanFactory parent) {
            owner = parent;
        }
        return owner;
    }

    private static boolean matches(String fromConfig, String actual) {
        return fromConfig == null || fromConfig.equals(actual);
    }
}
