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

package com.hazelcast.spring.jet;

import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.spring.context.SpringAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.Nonnull;

/**
 * Utility class with methods that create several useful Spring Bean
 * {@link ServiceFactory service factories} and transform functions.
 *
 * @since 4.0
 */
public final class JetSpringServiceFactories {

    private JetSpringServiceFactories() {
    }

    /**
     * Returns a Spring Bean {@link ServiceFactory}. The factory creates a
     * context object which autowires the {@link ApplicationContext}. The
     * context object obtains the specified bean from {@link ApplicationContext}
     * and returns it as a service.
     * <p>
     * Below is a sample usage which reads the names from a list and maps those
     * names to {@code User} objects using the spring bean {@code userDao}.
     * <pre>{@code
     * Pipeline pipeline = Pipeline.create();
     * pipeline.<String>readFrom(Sources.list(LIST_NAME))
     *         .mapUsingService(JetSpringServiceFactories.bean("userDao", UserDao.class),
     *                 (userDao, item) -> userDao.findByName(item.toLowerCase(Locale.ROOT)))
     *         .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param beanName     the name of the bean
     * @param requiredType the class of the bean
     * @param <T>          the type of the bean
     */
    public static <T> ServiceFactory<?, T> bean(@Nonnull String beanName, @Nonnull Class<T> requiredType) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                             .withCreateServiceFn((c, g) -> g.getBean(beanName, requiredType));
    }

    /**
     * Returns a Spring Bean {@link ServiceFactory}. The factory creates a
     * context object which autowires the {@link ApplicationContext}. The
     * context object obtains the specified bean from {@link ApplicationContext}
     * and returns it as a service.
     * <p>
     * Below is a sample usage which reads the names from a list and maps those
     * names to {@code User} objects using the spring bean {@code userDao}.
     * <pre>{@code
     * Pipeline pipeline = Pipeline.create();
     * pipeline.<String>readFrom(Sources.list(LIST_NAME))
     *         .mapUsingService(JetSpringServiceFactories.bean(UserDao.class),
     *                 (userDao, item) -> userDao.findByName(item.toLowerCase(Locale.ROOT)))
     *         .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param requiredType the class of the bean
     * @param <T>          the type of the bean
     */
    public static <T> ServiceFactory<?, T> bean(@Nonnull Class<T> requiredType) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                             .withCreateServiceFn((c, g) -> g.getBean(requiredType));
    }

    /**
     * Returns a Spring Bean {@link ServiceFactory}. The factory creates a
     * context object which autowires the {@link ApplicationContext}. The
     * context object obtains the specified bean from {@link ApplicationContext}
     * and returns it as a service.
     * <p>
     * Below is a sample usage which reads the names from a list and maps those
     * names to {@code User} objects using the spring bean {@code userDao}.
     * <pre>{@code
     * Pipeline pipeline = Pipeline.create();
     * pipeline.<String>readFrom(Sources.list(LIST_NAME))
     *         .mapUsingService(JetSpringServiceFactories.bean("userDao"),
     *                 (userDao, item) -> userDao.findByName(item.toLowerCase(Locale.ROOT)))
     *         .writeTo(Sinks.logger());
     * }</pre>
     *
     * @param beanName the name of the bean
     * @param <T>      the type of the bean
     */
    public static <T> ServiceFactory<?, T> bean(@Nonnull String beanName) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                             .withCreateServiceFn((c, g) -> g.getBean(beanName));
    }

    @SpringAware
    private static final class BeanExtractor {

        @Autowired
        private transient ApplicationContext context;

        public <T> T getBean(String name) {
            checkContext();
            return (T) context.getBean(name);
        }

        public <T> T getBean(String name, Class<T> type) {
            checkContext();
            return context.getBean(name, type);
        }

        public <T> T getBean(Class<T> type) {
            checkContext();
            return context.getBean(type);
        }

        private void checkContext() {
            if (context == null) {
                throw new IllegalStateException("The spring managed context is not configured properly");
            }
        }
    }
}
