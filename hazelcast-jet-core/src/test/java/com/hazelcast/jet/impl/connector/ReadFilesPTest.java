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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadFilesPTest extends SimpleTestInClusterSupport {

    private File directory;
    private IList<Entry<String, String>> list;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setup() throws Exception {
        directory = createTempDirectory();
        list = instance().getList("writer");
    }

    @Test
    public void test_smallFiles() throws Exception {
        Pipeline p = buildDag(null);

        File file1 = new File(directory, randomName());
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, randomName());
        appendToFile(file2, "hello2", "world2");

        instance().newJob(p).join();

        assertEquals(4, list.size());

        finishDirectory(file1, file2);
    }

    @Test
    public void test_largeFile() throws Exception {
        Pipeline p = buildDag(null);

        File file1 = new File(directory, randomName());
        final int listLength = 10000;
        appendToFile(file1, IntStream.range(0, listLength).mapToObj(String::valueOf).toArray(String[]::new));

        instance().newJob(p).join();

        assertEquals(listLength, list.size());
    }

    @Test
    public void when_glob_the_useGlob() throws Exception {
        Pipeline p = buildDag("file2.*");

        File file1 = new File(directory, "file1.txt");
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, "file2.txt");
        appendToFile(file2, "hello2", "world2");

        instance().newJob(p).join();

        assertEquals(Arrays.asList(entry("file2.txt", "hello2"), entry("file2.txt", "world2")), new ArrayList<>(list));

        finishDirectory(file1, file2);
    }

    @Test
    public void when_directory_then_ignore() {
        Pipeline p = buildDag(null);

        File file1 = new File(directory, randomName());
        assertTrue(file1.mkdir());

        instance().newJob(p).join();

        assertEquals(0, list.size());

        finishDirectory(file1);
    }

    private Pipeline buildDag(String glob) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.filesBuilder(directory.getPath())
                          .glob(glob == null ? "*" : glob)
                          .build(Util::entry))
         .writeTo(Sinks.list(list));

        return p;
    }

    private void finishDirectory(File ... files) {
        for (File file : files) {
            assertTrue(file.delete());
        }
        assertTrue(directory.delete());
    }
}
