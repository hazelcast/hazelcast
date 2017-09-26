/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.accumulator;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Hazelcast serializer hooks for the classes in the
 * {@code com.hazelcast.jet.accumulator} package.
 */
public class AccumulatorSerializerHooks {
    public static final class LongAccHook implements SerializerHook<LongAccumulator> {

        @Override
        public Class<LongAccumulator> getSerializationType() {
            return LongAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LongAccumulator>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.LONG_ACC;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, LongAccumulator object) throws IOException {
                    out.writeLong(object.get());
                }

                @Override
                public LongAccumulator read(ObjectDataInput in) throws IOException {
                    return new LongAccumulator(in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class DoubleAccHook implements SerializerHook<DoubleAccumulator> {

        @Override
        public Class<DoubleAccumulator> getSerializationType() {
            return DoubleAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<DoubleAccumulator>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.DOUBLE_ACC;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, DoubleAccumulator object) throws IOException {
                    out.writeDouble(object.get());
                }

                @Override
                public DoubleAccumulator read(ObjectDataInput in) throws IOException {
                    return new DoubleAccumulator(in.readDouble());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class MutableReferenceHook implements SerializerHook<MutableReference> {

        @Override
        public Class<MutableReference> getSerializationType() {
            return MutableReference.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<MutableReference>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.MUTABLE_REFERENCE;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, MutableReference object) throws IOException {
                    out.writeObject(object.get());
                }

                @Override
                public MutableReference read(ObjectDataInput in) throws IOException {
                    return new MutableReference<>(in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class LinTrendAccHook implements SerializerHook<LinTrendAccumulator> {

        @Override
        public Class<LinTrendAccumulator> getSerializationType() {
            return LinTrendAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LinTrendAccumulator>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.LIN_TREND_ACC;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, LinTrendAccumulator object) throws IOException {
                    object.writeObject(out);
                }

                @Override
                public LinTrendAccumulator read(ObjectDataInput in) throws IOException {
                    return new LinTrendAccumulator(
                            in.readLong(), readBigInt(in), readBigInt(in), readBigInt(in), readBigInt(in));
                }

                private BigInteger readBigInt(ObjectDataInput in) throws IOException {
                    byte[] bytes = new byte[in.readUnsignedByte()];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = in.readByte();
                    }
                    return new BigInteger(bytes);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class LongLongAccHook implements SerializerHook<LongLongAccumulator> {

        @Override
        public Class<LongLongAccumulator> getSerializationType() {
            return LongLongAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LongLongAccumulator>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.LONG_LONG_ACC;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, LongLongAccumulator object) throws IOException {
                    out.writeLong(object.getValue1());
                    out.writeLong(object.getValue2());
                }

                @Override
                public LongLongAccumulator read(ObjectDataInput in) throws IOException {
                    return new LongLongAccumulator(in.readLong(), in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class LongDoubleAccHook implements SerializerHook<LongDoubleAccumulator> {

        @Override
        public Class<LongDoubleAccumulator> getSerializationType() {
            return LongDoubleAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LongDoubleAccumulator>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.LONG_DOUBLE_ACC;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, LongDoubleAccumulator object) throws IOException {
                    out.writeLong(object.getValue1());
                    out.writeDouble(object.getValue2());
                }

                @Override
                public LongDoubleAccumulator read(ObjectDataInput in) throws IOException {
                    return new LongDoubleAccumulator(in.readLong(), in.readDouble());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
