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
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class AvroSourceTest extends JetTestSupport {

    private static final int TOTAL_RECORD_COUNT = 20;

    private static File directory;

    private JetInstance jet;
    private IListJet<? extends User> list;

    @BeforeClass
    public static void createDirectory() throws Exception {
        directory = createTempDirectory();
        createAvroFile(5);
        createAvroFile(15);
    }

    @AfterClass
    public static void cleanup() {
        IOUtil.delete(directory);
    }

    @Before
    public void setup() {
        jet = createJetMember();
        list = jet.getList("writer");
    }

    @Test
    public void testReflectReader() {
        Pipeline p = Pipeline.create();
        p.drawFrom(AvroSources.files(directory.getPath(), User.class, false))
         .drainTo(Sinks.list(list.getName()));

        jet.newJob(p).join();

        assertEquals(TOTAL_RECORD_COUNT, list.size());
    }

    @Test
    public void testSpecificReader() {
        Pipeline p = Pipeline.create();
        p.drawFrom(AvroSources.files(directory.getPath(), SpecificUser.class, false))
         .drainTo(Sinks.list(list.getName()));

        jet.newJob(p).join();

        assertEquals(TOTAL_RECORD_COUNT, list.size());
    }

    @Test
    public void testGenericReader() {
        Pipeline p = Pipeline.create();
        p.drawFrom(AvroSources.files(directory.getPath(), (file, record) -> toUser(record), false))
         .drainTo(Sinks.list(list.getName()));

        jet.newJob(p).join();

        assertEquals(TOTAL_RECORD_COUNT, list.size());
    }

    private static void createAvroFile(int recordCount) throws IOException {
        try (DataFileWriter<SpecificUser> writer = new DataFileWriter<>(new SpecificDatumWriter<>(SpecificUser.class))) {
            writer.create(SpecificUser.SCHEMA$, new File(directory, randomString()));
            for (int i = 0; i < recordCount; i++) {
                writer.append(new SpecificUser("username-" + i, "password-" + i));
            }
        }
    }

    private static User toUser(GenericRecord record) {
        return new User(record.get(0).toString(), record.get(1).toString());
    }
}
