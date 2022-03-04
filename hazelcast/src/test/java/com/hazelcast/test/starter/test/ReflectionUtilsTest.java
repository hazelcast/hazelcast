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

package com.hazelcast.test.starter.test;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.ReflectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Map;

import static com.hazelcast.test.starter.ReflectionUtils.getAllFieldsByName;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static com.hazelcast.test.starter.ReflectionUtils.isInstanceOf;
import static com.hazelcast.test.starter.ReflectionUtils.setFieldValueReflectively;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReflectionUtilsTest {

    private ReflectionTestClass testClass = new ReflectionTestClass();

    @Test
    public void testGetClass() {
        assertEquals(ReflectionTestClass.class, ReflectionUtils.getClass(testClass));
    }

    @Test
    public void testIsInstanceOf() {
        assertTrue("Expected testClass to be instanceOf ReflectionTestClass", isInstanceOf(testClass, ReflectionTestClass.class));
    }

    @Test
    public void testGetFieldValueReflectively() throws Exception {
        testClass.testString = "foobar";

        assertEquals("foobar", getFieldValueReflectively(testClass, "testString"));
    }

    @Test
    public void testGetFieldValueReflectively_whenNull() throws Exception {
        assertNull(getFieldValueReflectively(testClass, "testString"));
    }

    @Test(expected = NoSuchFieldError.class)
    public void testGetFieldValueReflectively_whenInvalidField() throws Exception {
        assertNull(getFieldValueReflectively(testClass, "invalidField"));
    }

    @Test
    public void testSetFieldValueReflectively() throws Exception {
        setFieldValueReflectively(testClass, "testInteger", 23);

        assertEquals(23, testClass.testInteger);
    }

    @Test(expected = NoSuchFieldError.class)
    public void testSetFieldValueReflectively_whenInvalidField() throws Exception {
        setFieldValueReflectively(testClass, "invalidField", 23);
    }

    @Test
    public void testGetAllFieldsByName() {
        Map<String, Field> fields = getAllFieldsByName(ReflectionTestClass.class);
        assertTrue("Expected at least 2 fields, but was " + fields.size() + " (" + fields + ")", fields.size() >= 2);
        assertTrue("Expected to find field 'testString' (" + fields + ")", fields.containsKey("testString"));
        assertTrue("Expected to find field 'testInteger' (" + fields + ")", fields.containsKey("testInteger"));

        Field stringField = fields.get("testString");
        assertEquals("Expected field 'testString' to be of type String", String.class, stringField.getType());
        assertFalse("Expected field 'testString' to be not accessible", stringField.isAccessible());

        Field integerField = fields.get("testInteger");
        assertEquals("Expected field 'testInteger' to be of type int", int.class, integerField.getType());
        assertFalse("Expected field 'testInteger' to be not accessible", integerField.isAccessible());
    }

    @SuppressWarnings("unused")
    private class ReflectionTestClass {

        private String testString;
        private int testInteger;
    }
}
