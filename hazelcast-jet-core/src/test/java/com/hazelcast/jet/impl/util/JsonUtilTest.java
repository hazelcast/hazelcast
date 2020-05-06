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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class JsonUtilTest extends JetTestSupport {

    static String jsonString;
    static TestJsonObject testJsonObject;

    @BeforeClass
    public static void setup() throws Exception {
        Path file = Paths.get(JsonUtilTest.class.getResource("file.json").toURI());
        jsonString = Files.lines(file)
                          .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                          .toString();
        testJsonObject = TestJsonObject.withDefaults();
    }

    @Test
    public void testParseToObject() {
        TestJsonObject jsonObject = JsonUtil.parse(TestJsonObject.class, jsonString);
        assertEquals(testJsonObject, jsonObject);
    }

    @Test
    public void testParseToMap() {
        Map<String, Object> objectMap = JsonUtil.parse(jsonString);

        assertEquals(6, objectMap.size());
        assertEquals(testJsonObject.name, objectMap.get("name"));
        assertEquals(testJsonObject.age, objectMap.get("age"));
        assertEquals(testJsonObject.status, objectMap.get("status"));
        assertEquals(Arrays.asList(testJsonObject.stringArray), objectMap.get("stringArray"));

        Map<String, Object> innerObject = (Map) objectMap.get("innerObject");
        assertEquals(1, innerObject.size());
        assertEquals(testJsonObject.innerObject.val, innerObject.get("val"));

        List<Map<String, Object>> objects = (List) objectMap.get("objects");
        List<String> expectedValueList = testJsonObject.objects.stream().map(i -> i.val).collect(toList());
        List<Object> valueList = objects.stream().filter(m -> m.size() == 1).map(m -> m.get("val")).collect(toList());
        assertEquals(expectedValueList, valueList);
    }

    @Test
    public void testExtractString() {
        String name = JsonUtil.getString(jsonString, "name");
        assertEquals(testJsonObject.name, name);
    }

    @Test
    public void testExtractInt() {
        int age = JsonUtil.getInt(jsonString, "age");
        assertEquals(testJsonObject.age, age);
    }

    @Test
    public void testExtractBoolean() {
        boolean status = JsonUtil.getBoolean(jsonString, "status");
        assertEquals(testJsonObject.status, status);
    }

    @Test
    public void testExtractList() {
        List<Object> objects = JsonUtil.getList(jsonString, "stringArray");
        assertEquals(Arrays.asList(testJsonObject.stringArray), objects);
    }

    @Test
    public void testExtractArray() {
        Object[] objects = JsonUtil.getArray(jsonString, "stringArray");
        assertArrayEquals(testJsonObject.stringArray, objects);
    }

    @Test
    public void testExtractObject() {
        Map<String, Object> innerObject = JsonUtil.getObject(jsonString, "innerObject");
        assertEquals(1, innerObject.size());
        assertEquals(testJsonObject.innerObject.val, innerObject.get("val"));
    }

    @Test
    public void testConvertToJsonString() {
        assertEquals(jsonString, JsonUtil.asString(testJsonObject));
    }

    public static class TestJsonObject {

        public String name;
        public int age = 8;
        public boolean status;
        public String[] stringArray;
        public List<InnerTestJsonObject> objects;
        public InnerTestJsonObject innerObject;

        public TestJsonObject() {
        }

        public static TestJsonObject withDefaults() {
            TestJsonObject jsonObject = new TestJsonObject();
            jsonObject.name = "foo";
            jsonObject.age = 8;
            jsonObject.status = true;
            jsonObject.stringArray = new String[]{"a", "b", "c", "d"};
            jsonObject.objects = Arrays.asList(new InnerTestJsonObject("x"), new InnerTestJsonObject("y"));
            jsonObject.innerObject = new InnerTestJsonObject("z");
            return jsonObject;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestJsonObject)) {
                return false;
            }
            TestJsonObject that = (TestJsonObject) o;
            return age == that.age &&
                    status == that.status &&
                    Objects.equals(name, that.name) &&
                    Arrays.equals(stringArray, that.stringArray) &&
                    Objects.equals(objects, that.objects) &&
                    Objects.equals(innerObject, that.innerObject);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name, age, status, objects, innerObject);
            result = 31 * result + Arrays.hashCode(stringArray);
            return result;
        }
    }


    public static class InnerTestJsonObject {

        public String val;

        public InnerTestJsonObject() {
        }

        public InnerTestJsonObject(String val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof InnerTestJsonObject)) {
                return false;
            }
            InnerTestJsonObject that = (InnerTestJsonObject) o;
            return Objects.equals(val, that.val);
        }

        @Override
        public int hashCode() {
            return Objects.hash(val);
        }
    }

}
