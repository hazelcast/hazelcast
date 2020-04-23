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

package com.hazelcast.jet.examples.protobuf;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.protobuf.Messages.Broker;
import com.hazelcast.jet.protobuf.ProtobufSerializer;

/**
 * Demonstrates the usage of Protobuf serializer adapter.
 * <p>
 * {@link BrokerSerializer} is registered on job level and then used to
 * serialize local {@link com.hazelcast.collection.IList} items.
 */
public class ProtobufSerializerAdapter {

    private static final String LIST_NAME = "brokers";

    private JetInstance jet;

    public static void main(String[] args) {
        new ProtobufSerializerAdapter().go();
    }

    private void go() {
        try {
            setup();

            JobConfig config = new JobConfig()
                    .registerSerializer(Broker.class, BrokerSerializer.class);
            jet.newJob(buildPipeline(), config).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        jet = Jet.bootstrappedInstance();
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.readFrom(TestSources.items(1, 2, 3, 4, 5))
         .map(id -> Broker.newBuilder().setId(id).build())
         .writeTo(Sinks.list(LIST_NAME));

        return p;
    }

    @SuppressWarnings("unused")
    private static class BrokerSerializer extends ProtobufSerializer<Broker> {

        private static final int TYPED_ID = 13;

        BrokerSerializer() {
            super(Broker.class, TYPED_ID);
        }
    }
}
