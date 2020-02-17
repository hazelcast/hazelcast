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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@OperationsPerInvocation(SerializationBenchmark.BATCH_SIZE)
public class SerializationBenchmark {

    static final int BATCH_SIZE = 1_000;

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
    public Object[] serializable() throws Exception {
        byte[] serialized = serializableSerializer.serialize(serializable);
        return serializableSerializer.deserialize(serialized);
    }

    @Benchmark
    public Object[] externalizable() throws Exception {
        byte[] serialized = externalizableSerializer.serialize(externalizable);
        return externalizableSerializer.deserialize(serialized);
    }

    @Benchmark
    public Object[] dataSerializable() throws Exception {
        byte[] serialized = dataSerializableSerializer.serialize(dataSerializable);
        return dataSerializableSerializer.deserialize(serialized);
    }

    @Benchmark
    public Object[] identifiedDataSerializable() throws Exception {
        byte[] serialized = identifiedDataSerializableSerializer.serialize(identifiedDataSerializable);
        return identifiedDataSerializableSerializer.deserialize(serialized);
    }

    @Benchmark
    public Object[] portable() throws Exception {
        byte[] serialized = portableSerializer.serialize(portable);
        return portableSerializer.deserialize(serialized);
    }

    @Benchmark
    public Object[] stream() throws Exception {
        byte[] serialized = streamSerializer.serialize(object);
        return streamSerializer.deserialize(serialized);
    }

    private static class Serializer {

        private final InternalSerializationService serializationService;

        Serializer(InternalSerializationService serializationService) {
            this.serializationService = serializationService;
        }

        byte[] serialize(Object object) throws IOException {
            try (BufferObjectDataOutput output = serializationService.createObjectDataOutput()) {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    serializationService.writeObject(output, object);
                }
                return output.toByteArray();
            }
        }

        Object[] deserialize(byte[] bytes) throws IOException {
            Object[] result = new Object[BATCH_SIZE];
            try (BufferObjectDataInput input = serializationService.createObjectDataInput(bytes)) {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    result[i] = serializationService.readObject(input);
                }
                return result;
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
