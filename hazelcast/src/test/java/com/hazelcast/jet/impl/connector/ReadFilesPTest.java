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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadFilesPTest extends SimpleTestInClusterSupport {

    private File directory;
    private IList<Entry<String, String>> list;
    private IList<Object> listJson;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setup() throws Exception {
        directory = createTempDirectory();
        list = instance().getList("writer");
        listJson = instance().getList("writerJson");
    }

    @Test
    public void test_smallFiles() throws Exception {
        Pipeline p = pipeline(null);

        File file1 = new File(directory, randomName());
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, randomName());
        appendToFile(file2, "hello2", "world2");

        instance().getJet().newJob(p).join();

        assertEquals(4, list.size());

        finishDirectory(file1, file2);
    }

    @Test
    public void test_largeFile() throws Exception {
        Pipeline p = pipeline(null);

        File file1 = new File(directory, randomName());
        final int listLength = 10000;
        appendToFile(file1, IntStream.range(0, listLength).mapToObj(String::valueOf).toArray(String[]::new));

        instance().getJet().newJob(p).join();

        assertEquals(listLength, list.size());

        finishDirectory(file1);
    }

    @Test
    public void when_glob_the_useGlob() throws Exception {
        Pipeline p = pipeline("file2.*");

        File file1 = new File(directory, "file1.txt");
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, "file2.txt");
        appendToFile(file2, "hello2", "world2");

        instance().getJet().newJob(p).join();

        assertEquals(Arrays.asList(entry("file2.txt", "hello2"), entry("file2.txt", "world2")), new ArrayList<>(list));

        finishDirectory(file1, file2);
    }

    @Test
    public void when_directory_then_ignore() {
        Pipeline p = pipeline(null);

        File file1 = new File(directory, randomName());
        assertTrue(file1.mkdir());

        instance().getJet().newJob(p).join();

        assertEquals(0, list.size());

        finishDirectory(file1);
    }

    @Test
    public void testJsonFilesOneLineItems_when_asObject_thenObjects() throws IOException {
        testJsonFiles_when_asObject_thenObjects(false);
    }

    @Test
    public void testJsonFilesMultilineItems_when_asObject_thenObjects() throws IOException {
        testJsonFiles_when_asObject_thenObjects(true);
    }

    @Test
    public void testJsonFilesOneLineItems_when_asMap_thenMaps() throws IOException {
        testJsonFiles_when_asMap_thenMaps(false);
    }

    @Test
    public void testJsonFilesMultilineItems_when_asMap_thenMaps() throws IOException {
        testJsonFiles_when_asMap_thenMaps(true);
    }

    private void testJsonFiles_when_asObject_thenObjects(boolean prettyPrinted) throws IOException {
        File[] jsonFiles = createJsonFiles(prettyPrinted);

        Pipeline p = pipelineJson(false);
        instance().getJet().newJob(p).join();

        assertEquals(4, listJson.size());
        TestPerson testPerson = (TestPerson) listJson.get(0);
        assertTrue(testPerson.name.startsWith("hello"));

        finishDirectory(jsonFiles);
    }

    private void testJsonFiles_when_asMap_thenMaps(boolean prettyPrinted) throws IOException {
        File[] jsonFiles = createJsonFiles(prettyPrinted);

        Pipeline p = pipelineJson(true);
        instance().getJet().newJob(p).join();

        assertEquals(4, listJson.size());
        Map<String, Object> testPersonMap = (Map) listJson.get(0);
        assertTrue(testPersonMap.get("name").toString().startsWith("hello"));

        finishDirectory(jsonFiles);
    }

    private File[] createJsonFiles(boolean prettyPrinted) throws IOException {
        File file1 = new File(directory, randomName() + ".json");
        String jsonItem1 = prettyPrinted
                ? "{\n"
                + "    \"name\": \"hello world\",\n"
                + "    \"age\": 5,\n"
                + "    \"status\": true\n"
                + "}"
                : "{\"name\": \"hello world\", \"age\": 5, \"status\": true}";
        String jsonItem2 = prettyPrinted
                ? "{\n"
                + "    \"name\": \"hello jupiter\",\n"
                + "    \"age\": 8,\n"
                + "    \"status\": false\n"
                + "}"
                : "{\"name\": \"hello jupiter\", \"age\": 8, \"status\": false}";
        appendToFile(file1, jsonItem1, jsonItem1);
        File file2 = new File(directory, randomName() + ".json");
        appendToFile(file2, jsonItem2, jsonItem2);
        return new File[]{file1, file2};
    }

    private Pipeline pipeline(String glob) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.filesBuilder(directory.getPath())
                          .glob(glob == null ? "*" : glob)
                          .build(Util::entry))
         .writeTo(Sinks.list(list));

        return p;
    }

    private Pipeline pipelineJson(boolean asMap) {
        Pipeline p = Pipeline.create();
        BatchSource<?> source  = asMap ? Sources.json(directory.getPath()) :
                Sources.json(directory.getPath(), TestPerson.class);
        p.readFrom(source)
         .writeTo(Sinks.list(listJson));

        return p;
    }

    private void finishDirectory(File... files) {
        for (File file : files) {
            assertTrue(file.delete());
        }
        assertTrue(directory.delete());
    }

    public static class TestPerson implements Serializable {

        public String name;
        public int age;
        public boolean status;

        public TestPerson() {
        }

        public TestPerson(String name, int age, boolean status) {
            this.name = name;
            this.age = age;
            this.status = status;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPerson that = (TestPerson) o;
            return age == that.age &&
                    status == that.status &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age, status);
        }
    }
}
