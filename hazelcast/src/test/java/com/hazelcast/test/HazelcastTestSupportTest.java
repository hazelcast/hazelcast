/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastTestSupportTest extends HazelcastTestSupport {

    @Test
    public void test_setPublicStaticFinalField() {
        setFieldValue(TestClass.class, "publicStaticFinalField", TRUE);
        assertEquals(true, TestClass.publicStaticFinalField);
    }

    @Test
    public void test_setPrivateStaticFinalField() {
        setFieldValue(TestClass.class, "privateStaticFinalField", TRUE);
        assertEquals(true, TestClass.privateStaticFinalField);
    }

    @Test
    public void test_setPublicStaticField() {
        setFieldValue(TestClass.class, "publicStaticField", TRUE);
        assertEquals(true, TestClass.publicStaticField);
    }

    @Test
    public void test_setPrivateStaticField() {
        setFieldValue(TestClass.class, "privateStaticField", TRUE);
        assertEquals(true, TestClass.privateStaticField);
    }

    @Test
    public void test_setPublicFinalField() {
        TestClass test = new TestClass();
        setFieldValue(test, "publicFinalField", TRUE);
        assertEquals(true, test.publicFinalField);
    }

    @Test
    public void test_setPrivateFinalField() {
        TestClass test = new TestClass();
        setFieldValue(test, "privateFinalField", TRUE);
        assertEquals(true, test.privateFinalField);
    }

    @Test
    public void test_setPublicField() {
        TestClass test = new TestClass();
        setFieldValue(test, "publicField", TRUE);
        assertEquals(true, test.publicField);
    }

    @Test
    public void test_setPrivateField() {
        TestClass test = new TestClass();
        setFieldValue(test, "privateField", TRUE);
        assertEquals(true, test.privateField);
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static final class TestClass {
        public static final Object publicStaticFinalField = FALSE;
        private static final Object privateStaticFinalField = FALSE;

        public static Object publicStaticField = FALSE;
        private static Object privateStaticField = FALSE;

        public final Object publicFinalField = FALSE;
        private final Object privateFinalField = FALSE;

        public Object publicField = FALSE;
        private Object privateField = FALSE;
    }
}
