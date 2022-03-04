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

package com.hazelcast.jet.impl.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonUtilTest extends JetTestSupport {

    static String jsonString;
    static String jsonStringList;
    static String jsonStringPrettyPrinted;
    static String jsonStringListPrettyPrinted;
    static TestJsonObject testJsonObject;

    @BeforeClass
    public static void setup() throws Exception {
        Path file = Paths.get(JsonUtilTest.class.getResource("file.json").toURI());
        jsonString = Files.lines(file)
                          .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                          .toString();

        Path fileList = Paths.get(JsonUtilTest.class.getResource("file_list.json").toURI());
        jsonStringList = Files.lines(fileList)
                              .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                              .toString();

        Path filePrettyPrinted = Paths.get(JsonUtilTest.class.getResource("file_pretty_printed.json").toURI());
        jsonStringPrettyPrinted = Files.lines(filePrettyPrinted)
                                       .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                                       .toString();

        Path fileListPrettyPrinted = Paths.get(JsonUtilTest.class.getResource("file_list_pretty_printed.json").toURI());
        jsonStringListPrettyPrinted = Files.lines(fileListPrettyPrinted)
                                           .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                                           .toString();
        testJsonObject = TestJsonObject.withDefaults();
    }

    @Test
    public void when_inputString_then_parseToObject() throws IOException {
        TestJsonObject jsonObject = JsonUtil.beanFrom(jsonString, TestJsonObject.class);
        assertEquals(testJsonObject, jsonObject);
    }

    @Test
    public void when_inputStringPrettyPrint_then_parseToObject() throws IOException {
        TestJsonObject jsonObject = JsonUtil.beanFrom(jsonStringPrettyPrinted, TestJsonObject.class);
        assertEquals(testJsonObject, jsonObject);
    }

    @Test
    public void when_inputString_then_parseToObjectWithAnnotation() throws IOException {
        TestJsonObjectWithAnnotations jsonObject = JsonUtil.beanFrom(jsonString, TestJsonObjectWithAnnotations.class);
        assertEquals(TestJsonObjectWithAnnotations.withDefaults(), jsonObject);
    }

    @Test
    public void when_inputStringPrettyPrint_then_parseToObjectWithAnnotation() throws IOException {
        TestJsonObjectWithAnnotations jsonObject
                = JsonUtil.beanFrom(jsonStringPrettyPrinted, TestJsonObjectWithAnnotations.class);
        assertEquals(TestJsonObjectWithAnnotations.withDefaults(), jsonObject);
    }

    @Test
    public void when_inputString_then_parseToMap() throws IOException {
        Map<String, Object> map = JsonUtil.mapFrom(jsonString);
        assertTestObjectAsMap(map, testJsonObject);
    }

    @Test
    public void when_inputStringPrettyPrint_then_parseToMap() throws IOException {
        Map<String, Object> map = JsonUtil.mapFrom(jsonStringPrettyPrinted);
        assertTestObjectAsMap(map, testJsonObject);
    }

    @Test
    public void when_inputString_then_parseToListOfObject() throws IOException {
        List<TestJsonObject> list = JsonUtil.listFrom(jsonStringList, TestJsonObject.class);
        assertListOfObjects(list);
    }

    @Test
    public void when_inputStringPrettyPrint_then_parseToListOfObject() throws IOException {
        List<TestJsonObject> list = JsonUtil.listFrom(jsonStringListPrettyPrinted, TestJsonObject.class);
        assertListOfObjects(list);
    }

    @Test
    public void when_inputString_then_parseToList() throws IOException {
        List<Object> list = JsonUtil.listFrom(jsonStringList);
        assertListOfMap(list);
    }

    @Test
    public void when_inputStringPrettyPrint_then_parseToList() throws IOException {
        List<Object> list = JsonUtil.listFrom(jsonStringListPrettyPrinted);
        assertListOfMap(list);
    }

    @Test
    public void when_inputString_and_contentObject_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom(jsonString);
        assertTestObjectAsMap((Map) o, testJsonObject);
    }

    @Test
    public void when_inputStringPrettyPrint_and_contentObject_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom(jsonStringPrettyPrinted);
        assertTestObjectAsMap((Map) o, testJsonObject);
    }

    @Test
    public void when_inputString_and_contentList_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom(jsonStringList);
        assertListOfMap((List) o);
    }

    @Test
    public void when_inputStringPrettyPrint_and_contentList_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom(jsonStringListPrettyPrinted);
        assertListOfMap((List) o);
    }

    @Test
    public void when_inputString_and_contentString_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom("\"abc\"");
        assertEquals("abc", o);
    }

    @Test
    public void when_inputString_and_contentInt_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom("10");
        assertEquals(10, (int) o);
    }

    @Test
    public void when_inputString_and_contentDouble_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom("10.5");
        assertEquals(10.5, (double) o, 0.0);
    }

    @Test
    public void when_inputString_and_contentBool_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom("true");
        assertTrue((boolean) o);
    }

    @Test
    public void when_inputString_and_contentNull_then_parseAny() throws IOException {
        Object o = JsonUtil.anyFrom("null");
        assertNull(o);
    }

    @Test
    public void when_inputReader_then_parseSequenceObject() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            sb.append(jsonString);
        }

        Iterator<TestJsonObject> iterator = JsonUtil.beanSequenceFrom(
                new StringReader(sb.toString()), TestJsonObject.class);

        assertIteratorObject(iterator, 20);
    }

    @Test
    public void when_mappingToObject_then_nullFieldsHandledProperly() throws IOException {
        long currentTimeMillis = System.currentTimeMillis();
        Birthdate birthdate1 = JsonUtil.beanFrom("{ \"date\" : " + currentTimeMillis + "  }", Birthdate.class);
        assertEquals(new Date(currentTimeMillis), birthdate1.date);

        Birthdate birthdate2 = JsonUtil.beanFrom("{ \"date\" : null }", Birthdate.class);
        assertNull(birthdate2.date);
    }

    private void assertIteratorObject(Iterator<TestJsonObject> iterator, int expectedCount) {
        int count = 0;
        while (iterator.hasNext()) {
            TestJsonObject next = iterator.next();
            assertEquals(testJsonObject, next);
            count++;
        }
        assertEquals(expectedCount, count);
    }

    @Test
    public void testConvertToJsonString() throws IOException {
        assertEquals(jsonString, JsonUtil.toJson(testJsonObject));
    }

    private void assertListOfObjects(List<TestJsonObject> list) {
        assertEquals(2, list.size());
        assertEquals(testJsonObject, list.get(0));
        TestJsonObject testJsonObject = TestJsonObject.withDefaults();
        testJsonObject.age = 2;
        assertEquals(testJsonObject, list.get(1));
    }

    private void assertListOfMap(List<Object> list) {
        assertEquals(2, list.size());
        Map<String, Object> o1 = (Map) list.get(0);
        assertTestObjectAsMap(o1, testJsonObject);
        Map<String, Object> o2 = (Map) list.get(1);
        TestJsonObject testJsonObject = TestJsonObject.withDefaults();
        testJsonObject.age = 2;
        assertTestObjectAsMap(o2, testJsonObject);
    }

    private void assertTestObjectAsMap(Map<String, Object> objectMap, TestJsonObject testJsonObject) {
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

    public static class TestJsonObject {

        public String name;
        public int age = 1;
        public boolean status;
        public String[] stringArray;
        public List<InnerTestJsonObject> objects;
        public InnerTestJsonObject innerObject;

        public TestJsonObject() {
        }

        public static TestJsonObject withDefaults() {
            TestJsonObject jsonObject = new TestJsonObject();
            jsonObject.name = "foo";
            jsonObject.age = 1;
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

    public static class TestJsonObjectWithAnnotations {

        @JsonProperty(value = "name")
        public String username;
        @JsonProperty(value = "age")
        public int userage = 1;

        public TestJsonObjectWithAnnotations() {
        }

        public static TestJsonObjectWithAnnotations withDefaults() {
            TestJsonObjectWithAnnotations jsonObject = new TestJsonObjectWithAnnotations();
            jsonObject.username = "foo";
            jsonObject.userage = 1;
            return jsonObject;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestJsonObjectWithAnnotations)) {
                return false;
            }
            TestJsonObjectWithAnnotations that = (TestJsonObjectWithAnnotations) o;
            return userage == that.userage && Objects.equals(username, that.username);
        }

        @Override
        public int hashCode() {
            return Objects.hash(username, userage);
        }

    }

    public static class Birthdate {
        public Date date;
    }

}
