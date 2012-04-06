/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.spring.context;

import com.hazelcast.core.ManagedContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 4/6/12
 */
public class SpringManagedContext implements ManagedContext, ApplicationContextAware {

    private final AtomicInteger idGen = new AtomicInteger();
    private AutowireCapableBeanFactory beanFactory;

    public SpringManagedContext() {
        super();
    }

    public void inspect(final Object obj) {
        if (obj != null) {
            Class clazz = obj.getClass();
            if (clazz.isAnnotationPresent(SpringAware.class)) {
                final String name = clazz.getName() + "#" + idGen.incrementAndGet();
                beanFactory.autowireBean(obj);
                beanFactory.initializeBean(obj, name);
            }
        }
    }

    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.beanFactory = applicationContext.getAutowireCapableBeanFactory();
    }
}
