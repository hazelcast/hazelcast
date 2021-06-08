/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.avro;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.avro.generated.SpecificUser;
import com.hazelcast.jet.avro.model.User;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AvroSinkTest extends JetTestSupport {

    private static final int TOTAL_RECORD_COUNT = 20;

    private HazelcastInstance hz;
    private File directory;
    private IList<User> list;

    @Before
    public void setup() throws Exception {
        hz = createHazelcastInstance();
        directory = createTempDirectory();
        list = hz.getList("writer");
        IntStream.range(0, TOTAL_RECORD_COUNT)
                 .mapToObj(i -> new User("name-" + i, i))
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
        p.readFrom(Sources.list(list))
         .writeTo(AvroSinks.files(directory.getPath(), User.class, User.classSchema()));

        hz.getJet().newJob(p).join();

        checkFileContent(new ReflectDatumReader<>(User.class));
    }

    @Test
    public void testSpecificWriter() throws IOException {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
         .map(user -> new SpecificUser(user.getName(), user.getFavoriteNumber()))
         .writeTo(AvroSinks.files(directory.getPath(), SpecificUser.class, SpecificUser.getClassSchema()));

        hz.getJet().newJob(p).join();

        checkFileContent(new SpecificDatumReader<>(SpecificUser.class));
    }

    @Test
    public void testGenericWriter() throws IOException {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
         .map(AvroSinkTest::toRecord)
         .writeTo(AvroSinks.files(directory.getPath(), User.classSchema()));

        hz.getJet().newJob(p).join();

        checkFileContent(new GenericDatumReader<>());
    }

    private <R> void checkFileContent(DatumReader<R> datumReader) throws IOException {
        File[] files = directory.listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
        int[] count = {0};
        try (DataFileReader<R> reader = new DataFileReader<>(files[0], datumReader)) {
            reader.forEach(datum -> count[0]++);
        }
        assertEquals(TOTAL_RECORD_COUNT, count[0]);
    }

    private static GenericRecord toRecord(User user) {
        Schema schema = ReflectData.get().getSchema(User.class);
        GenericRecord record = (GenericRecord) GenericData.get().newRecord(null, schema);
        record.put("name", user.getName());
        record.put("favoriteNumber", user.getFavoriteNumber());
        return record;
    }

}
