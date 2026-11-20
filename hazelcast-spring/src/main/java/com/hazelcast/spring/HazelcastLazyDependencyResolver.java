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
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.sql.SqlService;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.DependencyDescriptor;
import org.springframework.beans.factory.support.AutowireCandidateResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Allows a lazy exported dependency to be used from a consumer's initialization callback, even when Spring has not yet
 * instantiated the configuration that defines it. Installed by
 * {@link HazelcastObjectExtractionConfiguration#hzInternalLazyDependencyResolver()} before singleton initialization.
 *
 * <p>The existing resolver decides which dependencies are lazy and creates their proxies. Only proxies for distributed
 * objects, Jet and SQL services have their target sources decorated; qualifiers, values, and ordinary dependencies
 * retain the existing resolver's behavior. Target resolution and caching still belong to Spring's original target source.
 */
final class HazelcastLazyDependencyResolver implements AutowireCandidateResolver, BeanFactoryAware {

    private final AutowireCandidateResolver delegate;
    private ConfigurableListableBeanFactory beanFactory;

    HazelcastLazyDependencyResolver(AutowireCandidateResolver delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setBeanFactory(@Nonnull BeanFactory factory) {
        this.beanFactory = (ConfigurableListableBeanFactory) factory;
        if (delegate instanceof BeanFactoryAware aware) {
            aware.setBeanFactory(factory);
        }
    }

    @Override
    public boolean isAutowireCandidate(BeanDefinitionHolder holder, DependencyDescriptor descriptor) {
        return delegate.isAutowireCandidate(holder, descriptor);
    }

    @Override
    public boolean isRequired(DependencyDescriptor descriptor) {
        return delegate.isRequired(descriptor);
    }

    @Override
    public boolean hasQualifier(DependencyDescriptor descriptor) {
        return delegate.hasQualifier(descriptor);
    }

    @Nullable
    @Override
    public String getSuggestedName(DependencyDescriptor descriptor) {
        return delegate.getSuggestedName(descriptor);
    }

    @Nullable
    @Override
    public Object getSuggestedValue(DependencyDescriptor descriptor) {
        return delegate.getSuggestedValue(descriptor);
    }

    @Nullable
    @Override
    public Object getLazyResolutionProxyIfNecessary(DependencyDescriptor descriptor, @Nullable String beanName) {
        Object proxy = delegate.getLazyResolutionProxyIfNecessary(descriptor, beanName);
        Class<?> type = descriptor.getDependencyType();
        if (proxy instanceof Advised advised && (DistributedObject.class.isAssignableFrom(type)
                || type == JetService.class || type == SqlService.class)) {
            advised.setTargetSource(new ExportedObjectTargetSource(advised.getTargetSource()));
        }
        return proxy;
    }

    @Nullable
    @Override
    public Class<?> getLazyResolutionProxyClass(DependencyDescriptor descriptor, @Nullable String beanName) {
        return delegate.getLazyResolutionProxyClass(descriptor, beanName);
    }

    @Nonnull
    @Override
    public AutowireCandidateResolver cloneIfNecessary() {
        return new HazelcastLazyDependencyResolver(delegate.cloneIfNecessary());
    }

    private final class ExportedObjectTargetSource implements TargetSource {

        private final TargetSource targetSource;

        private ExportedObjectTargetSource(TargetSource targetSource) {
            this.targetSource = targetSource;
        }

        @Nullable
        @Override
        public Class<?> getTargetClass() {
            return targetSource.getTargetClass();
        }

        @Override
        public boolean isStatic() {
            return targetSource.isStatic();
        }

        /**
         * Tries ordinary resolution first, then initializes sources one at a time until an export becomes available.
         * Config beans precede members so registration itself need not start a member. Type discovery does not eagerly
         * initialize FactoryBeans, and recognizable clients are skipped. The explicit ambiguity catches are necessary
         * because {@link NoUniqueBeanDefinitionException} also extends {@link NoSuchBeanDefinitionException}.
         */
        @Nullable
        @Override
        public Object getTarget() throws Exception {
            NoSuchBeanDefinitionException missing;
            try {
                return targetSource.getTarget();
            } catch (NoUniqueBeanDefinitionException ambiguous) {
                throw ambiguous;
            } catch (NoSuchBeanDefinitionException notRegistered) {
                missing = notRegistered;
            }
            for (Class<?> sourceType : new Class<?>[]{Config.class, HazelcastInstance.class}) {
                String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory, sourceType, true, false);
                for (String name : names) {
                    ConfigurableListableBeanFactory owner = BeanCreator.owningFactory(beanFactory, name);
                    Class<?> type = owner.getType(name, false);
                    if (owner.isCurrentlyInCreation(name)
                            || type != null && HazelcastClientProxy.class.isAssignableFrom(type)) {
                        continue;
                    }
                    owner.getBean(name);
                    try {
                        return targetSource.getTarget();
                    } catch (NoUniqueBeanDefinitionException ambiguous) {
                        throw ambiguous;
                    } catch (NoSuchBeanDefinitionException notRegistered) {
                        missing = notRegistered;
                    }
                }
            }
            throw missing;
        }

        @Override
        public void releaseTarget(@Nonnull Object target) throws Exception {
            targetSource.releaseTarget(target);
        }
    }
}
