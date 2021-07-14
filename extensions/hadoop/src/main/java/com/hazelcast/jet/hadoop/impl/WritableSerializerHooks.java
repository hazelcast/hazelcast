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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

final class WritableSerializerHooks {

    private static final int DEFAULT = -380;
    private static final int INT = -381;
    private static final int LONG = -382;
    private static final int BOOLEAN = -383;
    private static final int FLOAT = -384;
    private static final int DOUBLE = -385;
    private static final int BYTE = -386;
    private static final int TEXT = -387;

    private WritableSerializerHooks() {
    }

    static final class Boolean extends WritableSerializerHook<BooleanWritable> {
        Boolean() {
            super(BooleanWritable.class, BooleanWritable::new, BOOLEAN);
        }
    }

    static final class Byte extends WritableSerializerHook<ByteWritable> {
        Byte() {
            super(ByteWritable.class, ByteWritable::new, BYTE);
        }
    }

    static final class Double extends WritableSerializerHook<DoubleWritable> {
        Double() {
            super(DoubleWritable.class, DoubleWritable::new, DOUBLE);
        }
    }

    static final class Float extends WritableSerializerHook<FloatWritable> {
        Float() {
            super(FloatWritable.class, FloatWritable::new, FLOAT);
        }
    }

    static final class Int extends WritableSerializerHook<IntWritable> {
        Int() {
            super(IntWritable.class, IntWritable::new, INT);
        }
    }

    static final class Long extends WritableSerializerHook<LongWritable> {
        Long() {
            super(LongWritable.class, LongWritable::new, LONG);
        }
    }

    static final class Text extends WritableSerializerHook<org.apache.hadoop.io.Text> {
        Text() {
            super(org.apache.hadoop.io.Text.class, org.apache.hadoop.io.Text::new, TEXT);
        }
    }

    static final class Default implements SerializerHook<Writable> {

        @Override
        public Class<Writable> getSerializationType() {
            return Writable.class;
        }

        @Override
        public Serializer createSerializer() {
            return new WritableStreamSerializer();
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    private static class WritableStreamSerializer implements StreamSerializer<Writable> {

        private static final ILogger LOGGER = Logger.getLogger(WritableStreamSerializer.class);

        private boolean warnedOnce;

        @Override
        public int getTypeId() {
            return DEFAULT;
        }

        @Override
        public void write(ObjectDataOutput out, Writable writable) throws IOException {
            if (!warnedOnce) {
                LOGGER.warning("Using default Writable serialization without explicit type registration. " +
                        "Consider explicit type registration using WritableSerializerHook.");
                warnedOnce = true;
            }
            out.writeUTF(writable.getClass().getName());
            writable.write(out);
        }

        @Override
        public Writable read(ObjectDataInput in) throws IOException {
            if (!warnedOnce) {
                LOGGER.warning("Using default Writable serialization without explicit type registration. " +
                        "Consider explicit type registration using WritableSerializerHook.");
                warnedOnce = true;
            }
            String className = in.readUTF();
            try {
                Writable instance = ClassLoaderUtil.newInstance(Thread.currentThread().getContextClassLoader(), className);
                instance.readFields(in);
                return instance;
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }
}

