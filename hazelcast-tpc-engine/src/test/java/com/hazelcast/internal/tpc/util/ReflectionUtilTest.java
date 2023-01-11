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

package com.hazelcast.internal.tpc.util;

import org.junit.Test;

import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;
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
        ClassWithStaticField.staticField = Integer.valueOf(255);
        Integer value = findStaticFieldValue(ClassWithStaticField.class, "staticField");
        assertEquals(ClassWithStaticField.staticField, value);
    }

    public static class ClassWithStaticField {
        public static Integer staticField;
    }
}
