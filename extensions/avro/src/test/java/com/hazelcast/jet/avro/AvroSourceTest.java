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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.avro.generated.SpecificUser;
import com.hazelcast.jet.avro.model.User;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class AvroSourceTest extends SimpleTestInClusterSupport {

    private static final int TOTAL_RECORD_COUNT = 20;

    private File directory;
    private IList<? extends User> list;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void createDirectory() throws Exception {
        directory = createTempDirectory();

        list = instance().getList(randomName());
    }

    @After
    public void cleanup() {
        IOUtil.delete(directory);
    }

    @Test
    public void testReflectReader() throws IOException {
        createAvroFiles(new ReflectDatumWriter<>(User.class), User.classSchema(), i -> new User("name-" + i, i));

        Pipeline p = Pipeline.create();
        p.readFrom(AvroSources.files(directory.getPath(), User.class))
         .writeTo(Sinks.list(list.getName()));

        instance().getJet().newJob(p).join();

        assertEquals(TOTAL_RECORD_COUNT, list.size());
    }

    @Test
    public void testSpecificReader() throws IOException {
        createAvroFiles(new SpecificDatumWriter<>(SpecificUser.class), SpecificUser.getClassSchema(),
                i -> new SpecificUser("name-" + i, i));

        Pipeline p = Pipeline.create();
        p.readFrom(AvroSources.files(directory.getPath(), SpecificUser.class))
         .writeTo(Sinks.list(list.getName()));

        instance().getJet().newJob(p).join();

        assertEquals(TOTAL_RECORD_COUNT, list.size());
    }

    @Test
    public void testGenericReader() throws IOException {
        createAvroFiles(new GenericDatumWriter<>(), User.classSchema(), AvroSourceTest::record);

        Pipeline p = Pipeline.create();
        p.readFrom(AvroSources.files(directory.getPath(), (file, record) -> toUser(record)))
         .writeTo(Sinks.list(list.getName()));

        instance().getJet().newJob(p).join();

        assertEquals(TOTAL_RECORD_COUNT, list.size());
    }

    private <R> void createAvroFiles(DatumWriter<R> datumWriter, Schema schema, Function<Integer, R> datumFn)
            throws IOException {
        createAvroFile(datumWriter, schema, datumFn, TOTAL_RECORD_COUNT / 2);
        createAvroFile(datumWriter, schema, datumFn, TOTAL_RECORD_COUNT / 2);
    }

    private <R> void createAvroFile(DatumWriter<R> datumWriter, Schema schema,
                                    Function<Integer, R> datumFn, int recordCount) throws IOException {
        try (DataFileWriter<R> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(schema, new File(directory, randomString()));
            for (int i = 0; i < recordCount; i++) {
                writer.append(datumFn.apply(i));
            }
        }
    }

    private static GenericRecord record(int i) {
        Schema schema = ReflectData.get().getSchema(User.class);
        GenericRecord record = (GenericRecord) GenericData.get().newRecord(null, schema);
        record.put("name", "name" + i);
        record.put("favoriteNumber", i);
        return record;
    }

    private static User toUser(GenericRecord record) {
        return new User(record.get("name").toString(), Integer.parseInt(record.get("favoriteNumber").toString()));
    }
}
