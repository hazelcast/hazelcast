/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * @leimer 8/15/12
 */
@SpringAware
public class SomeRunnableTask implements Runnable, Serializable, ApplicationContextAware {

    private static final long serialVersionUID = 5851289963628278937L;

    private transient ApplicationContext context;

    @Autowired
    private transient SomeBean someBean;

    public void run() {
        assertNotNull(someBean);
        assertNotNull(context);

        SomeBean bean = (SomeBean) context.getBean("someBean");
        assertEquals(someBean, bean);
    }

    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

}
