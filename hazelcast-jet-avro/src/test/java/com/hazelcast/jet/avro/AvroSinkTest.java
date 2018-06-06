/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.avro;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.avro.model.SpecificUser;
import com.hazelcast.jet.avro.model.User;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class AvroSinkTest extends JetTestSupport {

    private static final int TOTAL_RECORD_COUNT = 20;


    private JetInstance jet;
    private File directory;
    private IListJet<SpecificUser> list;

    @Before
    public void setup() throws Exception {
        jet = createJetMember();
        directory = createTempDirectory();
        list = jet.getList("writer");
        IntStream.range(0, TOTAL_RECORD_COUNT)
                 .mapToObj(i -> new SpecificUser("username-" + i, "password-" + i))
                 .forEach(user -> list.add(user));
    }

    @After
    public void cleanup() {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                assertTrue(file.delete());
            }
        }
        assertTrue(directory.delete());
    }

    @Test
    public void testReflectWriter() throws IOException {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<User>list(list.getName()))
         .drainTo(AvroSinks.files(directory.getPath(), () -> SpecificUser.SCHEMA$, User.class));

        jet.newJob(p).join();

        checkFileContent();
    }

    @Test
    public void testSpecificWriter() throws IOException {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<SpecificUser>list(list.getName()))
         .drainTo(AvroSinks.files(directory.getPath(), () -> SpecificUser.SCHEMA$, SpecificUser.class));

        jet.newJob(p).join();

        checkFileContent();
    }

    @Test
    public void testGenericWriter() throws IOException {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<SpecificUser>list(list.getName()))
         .drainTo(AvroSinks.files(directory.getPath(), () -> SpecificUser.SCHEMA$));

        jet.newJob(p).join();

        checkFileContent();
    }

    private void checkFileContent() throws IOException {
        File[] files = directory.listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
        int[] count = {0};
        try (DataFileReader<User> reader = new DataFileReader<>(files[0], new ReflectDatumReader<>(User.class))) {
            reader.forEach(user -> count[0]++);
        }
        assertEquals(TOTAL_RECORD_COUNT, count[0]);
    }

}
