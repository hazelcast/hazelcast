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

package com.hazelcast.jet.examples.files.avro;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.avro.AvroSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


/**
 * Demonstrates dumping a map's values to an Apache Avro file.
 */
public class AvroSink {

    public static final String MAP_NAME = "userMap";
    public static final String DIRECTORY_NAME;

    static {
        Path path = Paths.get(AvroSink.class.getClassLoader().getResource("").getPath());
        DIRECTORY_NAME = path.getParent().getParent().toString() + "/users";
    }

    private JetInstance jet;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        Schema schema = schemaForUser();
        p.readFrom(Sources.<String, User>map(MAP_NAME))
         .map(Map.Entry::getValue)
         .writeTo(AvroSinks.files(DIRECTORY_NAME, User.class, schema));

        return p;
    }

    public static void main(String[] args) throws Exception {
        new AvroSink().go();
    }

    private void go() {
        try {
            setup();
            jet.newJob(buildPipeline()).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        jet = Jet.bootstrappedInstance();

        IMap<String, User> map = jet.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            User user = new User("User" + i, "pass" + i, i, i % 2 == 0);
            map.put(user.getUsername(), user);
        }
    }

    private static Schema schemaForUser() {
        return SchemaBuilder.record(User.class.getSimpleName())
                            .namespace(User.class.getPackage().getName())
                            .fields()
                                .name("username").type().stringType().noDefault()
                                .name("password").type().stringType().noDefault()
                                .name("age").type().intType().noDefault()
                                .name("status").type().booleanType().noDefault()
                            .endRecord();
    }
}
