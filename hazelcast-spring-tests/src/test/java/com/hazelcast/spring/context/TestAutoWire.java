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

package com.hazelcast.spring.context;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests if hazelcast instance can be used/injected in a class with an {@code @Autowired}
 * annotation. This test specifically for {@code @Autowired} case, not other annotations like
 * {@code @Autowired
 * behave differently.
 * <p>
 * {@link org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor}
 * {@link org.springframework.context.annotation.CommonAnnotationBeanPostProcessor}
 */
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"test-application-context.xml"})
class TestAutoWire {

    @Autowired
    private ApplicationContext context;

    @BeforeAll
    @AfterAll
    static void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    void smoke() {
        SomeBeanHazelcastInjected bean = context.getBean(SomeBeanHazelcastInjected.class);
        assertNotNull(bean.getInstance());
    }
}
