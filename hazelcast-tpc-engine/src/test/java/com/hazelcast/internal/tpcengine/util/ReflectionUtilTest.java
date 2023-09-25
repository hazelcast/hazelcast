/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import static com.hazelcast.internal.tpcengine.util.ReflectionUtil.findStaticFieldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ReflectionUtilTest {

    @Test
    public void test_whenFieldNotExist() {
        Object value = findStaticFieldValue(ClassWithStaticField.class, "nonexisting");
        assertNull(value);
    }

    @Test
    public void test_whenFieldExist() {
        Integer value = findStaticFieldValue(ClassWithStaticField.class, "staticField");
        assertEquals(ClassWithStaticField.staticField, value);
    }

    @Test
    public void test_whenClassNotExist() {
        Object value = findStaticFieldValue("com.foo.bar.Baz", "nonexisting");
        assertNull(value);
    }

    @Test
    public void test_whenClassExist_butFieldDoesNot() {
        Integer value = findStaticFieldValue(ClassWithStaticField.class, "nonexisting");
        assertNull(value);
    }

    @Test
    public void test_whenClassAndFieldExist() {
        Integer value = findStaticFieldValue(ClassWithStaticField.class, "staticField");
        assertEquals(ClassWithStaticField.staticField, value);
    }

    public static class ClassWithStaticField {
        public static Integer staticField = 255;
    }
}
