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

package com.hazelcast.jet.examples.elastic;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.elastic.ElasticClients;
import com.hazelcast.jet.elastic.ElasticSinks;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.elasticsearch.action.index.IndexRequest;

import java.nio.file.Files;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.Pipeline.create;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example for Elastic Sink
 *
 * Reads files from `src/main/resources/documents` folder and indexes them as
 * documents into Elastic running on localhost:9200
 */
public class ElasticSinkExample {

    public static void main(String[] args) {
        try {
            Pipeline p = create();
            p.readFrom(files("src/main/resources/documents"))
             .map(JsonUtil::mapFrom)
             .writeTo(ElasticSinks.elastic(
                     () -> ElasticClients.client("localhost", 9200),
                     map -> new IndexRequest("my-index").source(map)
             ));

            JetInstance jet = Jet.bootstrappedInstance();
            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    public static BatchSource<String> files(String directory) {
        return Sources.filesBuilder(directory)
                      .build(path -> Stream.of(new String(Files.readAllBytes(path), UTF_8)));
    }

}
