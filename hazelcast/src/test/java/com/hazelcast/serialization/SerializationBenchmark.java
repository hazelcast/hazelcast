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

package com.hazelcast.serialization;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
public class SerializationBenchmark {

    Serializable serializable;
    Serializer serializableSerializer;

    Externalizable externalizable;
    Serializer externalizableSerializer;

    DataSerializable dataSerializable;
    Serializer dataSerializableSerializer;

    IdentifiedDataSerializable identifiedDataSerializable;
    Serializer identifiedDataSerializableSerializer;

    Portable portable;
    Serializer portableSerializer;

    Object object;
    Serializer streamSerializer;

    @Setup
    public void setup() {
        serializable = SerializableObject.object();
        serializableSerializer = new Serializer(SerializableObject.serializationService());

        externalizable = ExternalizableObject.object();
        externalizableSerializer = new Serializer(ExternalizableObject.serializationService());

        dataSerializable = DataSerializableObject.object();
        dataSerializableSerializer = new Serializer(DataSerializableObject.serializationService());

        identifiedDataSerializable = IdentifiedDataSerializableObject.object();
        identifiedDataSerializableSerializer = new Serializer(IdentifiedDataSerializableObject.serializationService());

        portable = PortableObject.object();
        portableSerializer = new Serializer(PortableObject.serializationService());

        object = StreamObject.object();
        streamSerializer = new Serializer(StreamObject.serializationService());
    }

    @Benchmark
    public void serializable(Blackhole blackhole) throws Exception {
        byte[] serialized = serializableSerializer.serialize(serializable);
        blackhole.consume(serializableSerializer.deserialize(serialized));
    }

    @Benchmark
    public void externalizable(Blackhole blackhole) throws Exception {
        byte[] serialized = externalizableSerializer.serialize(externalizable);
        blackhole.consume(externalizableSerializer.deserialize(serialized));
    }

    @Benchmark
    public void dataSerializable(Blackhole blackhole) throws Exception {
        byte[] serialized = dataSerializableSerializer.serialize(dataSerializable);
        blackhole.consume(dataSerializableSerializer.deserialize(serialized));
    }

    @Benchmark
    public void identifiedDataSerializable(Blackhole blackhole) throws Exception {
        byte[] serialized = identifiedDataSerializableSerializer.serialize(identifiedDataSerializable);
        blackhole.consume(identifiedDataSerializableSerializer.deserialize(serialized));
    }

    @Benchmark
    public void portable(Blackhole blackhole) throws Exception {
        byte[] serialized = portableSerializer.serialize(portable);
        blackhole.consume(portableSerializer.deserialize(serialized));
    }

    @Benchmark
    public void stream(Blackhole blackhole) throws Exception {
        byte[] serialized = streamSerializer.serialize(object);
        blackhole.consume(streamSerializer.deserialize(serialized));
    }

    private static class Serializer {

        private final InternalSerializationService serializationService;

        public Serializer(InternalSerializationService serializationService) {
            this.serializationService = serializationService;
        }

        byte[] serialize(Object object) throws IOException {
            try (BufferObjectDataOutput output = serializationService.createObjectDataOutput()) {
                serializationService.writeObject(output, object);
                return output.toByteArray();
            }
        }

        <T> T deserialize(byte[] bytes) throws IOException {
            try (BufferObjectDataInput input = serializationService.createObjectDataInput(bytes)) {
                return serializationService.readObject(input);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new OptionsBuilder()
                .include(SerializationBenchmark.class.getSimpleName())
                .build();

        new Runner(options).run();
    }
}
