/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.impl.util.ReflectionUtils.Resources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URL;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
public class ReflectionUtilsTest {

    @Test
    public void readStaticFieldOrNull_whenClassDoesNotExist_thenReturnNull() {
        Object field = ReflectionUtils.readStaticFieldOrNull("foo.bar.nonExistingClass", "field");
        assertNull(field);
    }

    @Test
    public void readStaticFieldOrNull_whenFieldDoesNotExist_thenReturnNull() {
        Object field = ReflectionUtils.readStaticFieldOrNull(MyClass.class.getName(), "nonExistingField");
        assertNull(field);
    }

    @Test
    public void readStaticFieldOrNull_readFromPrivateField() {
        String field = ReflectionUtils.readStaticFieldOrNull(MyClass.class.getName(), "staticPrivateField");
        assertEquals("staticPrivateFieldContent", field);
    }

    @Test
    public void readStaticFieldOrNull_readFromPublicField() {
        String field = ReflectionUtils.readStaticFieldOrNull(MyClass.class.getName(), "staticPublicField");
        assertEquals("staticPublicFieldContent", field);
    }

    @Test
    public void when_nestedClassesOf_then_returnsAllNestedClasses() throws ClassNotFoundException {
        // When
        Collection<Class<?>> classes = ReflectionUtils.nestedClassesOf(OuterClass.class);

        // Then
        assertThat(classes, hasSize(3));
        assertThat(classes, containsInAnyOrder(
                OuterClass.class,
                OuterClass.NestedClass.class,
                Class.forName("com.hazelcast.jet.impl.util.ReflectionUtilsTest$OuterClass$1")
        ));
    }

    @Test
    public void when_resourcesOf_then_returnsAllResources() throws ClassNotFoundException {
        // When
        Resources resources = ReflectionUtils.resourcesOf(OuterClass.class.getPackage().getName());

        // Then
        Collection<Class<?>> classes = resources.classes().collect(toList());
        assertThat(classes, hasSize(greaterThan(3)));
        assertThat(classes, hasItem(OuterClass.class));
        assertThat(classes, hasItem(OuterClass.NestedClass.class));
        assertThat(classes, hasItem(Class.forName("com.hazelcast.jet.impl.util.ReflectionUtilsTest$OuterClass$1")));

        List<URL> nonClasses = resources.nonClasses().collect(toList());
        assertThat(nonClasses, hasSize(1));
        assertThat(nonClasses, hasItem(hasToString(containsString("package.properties"))));
    }

    @SuppressWarnings("unused")
    public static final class MyClass {
        public static String staticPublicField = "staticPublicFieldContent";
        private static String staticPrivateField = "staticPrivateFieldContent";
    }

    @SuppressWarnings("unused")
    private static class OuterClass {
        private void method() {
            new Object() {
            };
        }

        private static class NestedClass {
        }
    }
}
