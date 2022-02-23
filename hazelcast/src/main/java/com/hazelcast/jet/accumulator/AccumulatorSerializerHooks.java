/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigInteger;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.accumulator} package. This is not a public-facing API.
 */
final class AccumulatorSerializerHooks {

    /**
     * Private constructor
     */
    private AccumulatorSerializerHooks() {
    }

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
                public void write(@Nonnull ObjectDataOutput out, @Nonnull LongAccumulator object) throws IOException {
                    out.writeLong(object.get());
                }

                @Nonnull @Override
                public LongAccumulator read(@Nonnull ObjectDataInput in) throws IOException {
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
                public void write(@Nonnull ObjectDataOutput out, @Nonnull DoubleAccumulator object) throws IOException {
                    out.writeDouble(object.export());
                }

                @Nonnull @Override
                public DoubleAccumulator read(@Nonnull ObjectDataInput in) throws IOException {
                    return new DoubleAccumulator(in.readDouble());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    @SuppressWarnings("rawtypes")
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
                public void write(@Nonnull ObjectDataOutput out, @Nonnull MutableReference object) throws IOException {
                    out.writeObject(object.get());
                }

                @Nonnull @Override
                public MutableReference read(@Nonnull ObjectDataInput in) throws IOException {
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
                public void write(
                        @Nonnull ObjectDataOutput out, @Nonnull LinTrendAccumulator object
                ) throws IOException {
                    object.writeObject(out);
                }

                @Nonnull @Override
                public LinTrendAccumulator read(@Nonnull ObjectDataInput in) throws IOException {
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
                public void write(
                        @Nonnull ObjectDataOutput out, @Nonnull LongLongAccumulator object
                ) throws IOException {
                    out.writeLong(object.get1());
                    out.writeLong(object.get2());
                }

                @Nonnull
                @Override
                public LongLongAccumulator read(@Nonnull ObjectDataInput in) throws IOException {
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
                public void write(
                        @Nonnull ObjectDataOutput out, @Nonnull LongDoubleAccumulator object
                ) throws IOException {
                    out.writeLong(object.getLong());
                    out.writeDouble(object.getDouble());
                }

                @Nonnull @Override
                public LongDoubleAccumulator read(@Nonnull ObjectDataInput in) throws IOException {
                    return new LongDoubleAccumulator(in.readLong(), in.readDouble());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    @SuppressWarnings("rawtypes")
    public static final class PickAnyAccHook implements SerializerHook<PickAnyAccumulator> {

        @Override
        public Class<PickAnyAccumulator> getSerializationType() {
            return PickAnyAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<PickAnyAccumulator>() {

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.PICK_ANY_ACC;
                }

                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull PickAnyAccumulator acc) throws IOException {
                    out.writeObject(acc.get());
                    out.writeLong(acc.count());
                }

                @Nonnull @Override
                public PickAnyAccumulator read(@Nonnull ObjectDataInput in) throws IOException {
                    return new PickAnyAccumulator<>(in.readObject(), in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }
}
