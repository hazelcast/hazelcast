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

package com.hazelcast.spring.context;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.assertNotNull;

/**
 * Tests if hazelcast instance can be used/injected in a class with an {@code @Autowired}
 * annotation. This test specifically for {@code @Autowired} case, not other annotations like
 * {@code @Resource}. Because they are using different annotation bean post processors so they may
 * behave differently.
 * <p>
 * {@link org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor}
 * {@link org.springframework.context.annotation.CommonAnnotationBeanPostProcessor}
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"test-application-context.xml"})
@Category(QuickTest.class)
public class TestAutoWire {

    @Autowired
    private ApplicationContext context;

    @BeforeClass
    @AfterClass
    public static void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void smoke() {
        SomeBeanHazelcastInjected bean = context.getBean(SomeBeanHazelcastInjected.class);
        assertNotNull(bean.getInstance());
    }
}
