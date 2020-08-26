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

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A generic object interface that is returned to user when the domain class can not be created from any of the distributed
 * hazelcast data structures like {@link com.hazelcast.map.IMap} ,{@link com.hazelcast.collection.IQueue} etc.
 * <p>
 * On remote calls like distributed executor service or EntryProcessors, you may need to access to the domain object. In
 * case class of the domain object is not available on the cluster, GenericRecord allows to access, read and write the objects
 * back without the class of the domain object on the classpath. Here is an read example with EntryProcessor:
 * <pre>
 * map.executeOnKey(key, (EntryProcessor<Object, Object, Object>) entry -> {
 *             Object value = entry.getValue();
 *             GenericRecord genericRecord = (GenericRecord) value;
 *
 *             int id = genericRecord.readInt("id");
 *
 *             return null;
 *         });
 * </pre>
 * Another example with EntryProcessor to demonstrate how to read, modify and set back to the map:
 * <pre>
 * map.executeOnKey("key", (EntryProcessor<Object, Object, Object>) entry -> {
 *             GenericRecord genericRecord = (GenericRecord) entry.getValue();
 *             GenericRecord modifiedGenericRecord = genericRecord.cloneWithBuilder()
 *                     .writeInt("age",22).build();
 *             entry.setValue(modifiedGenericRecord);
 *             return null;
 *         });
 * </pre>
 * <p>
 * GenericRecord also allows to read from a cluster without having the classes on the client side.
 * For {@link Portable}, when {@link PortableFactory} is not provided in the config at the start,
 * a {@link HazelcastSerializationException} was thrown stating that a factory could not be found. Starting from 4.1,
 * the objects will be returned as {@link GenericRecord}. This way, the clients can be  read and write the objects back to
 * the cluster without needing the classes of the domain objects on the classpath.
 * <p>
 * Currently this is valid for {@link Portable} objects.
 *
 * @since 4.1
 */
@Beta
public interface GenericRecord {

    /**
     * Creates a {@link Builder} allows to create a new object. This method is a convenience method to get a builder,
     * without creating the class definition for this type. Here you can see  a  new object is constructed from an existing
     * GenericRecord with its class definition:
     *
     * <pre>
     *
     * GenericRecord newGenericRecord = genericRecord.newBuilder()
     *      .writeUTF("name", "bar")
     *      .writeInt("id", 4).build();
     *
     * </pre>
     * <p>
     * see {@link Builder#portable(ClassDefinition)} to create a GenericRecord in Portable format
     * with a different class definition.
     *
     * @return an empty generic record builder with same class definition as this one
     */
    @Nonnull
    Builder newBuilder();

    /**
     * Returned {@link Builder} can be used to have exact copy and also just to update a couple of fields. By default,
     * it will copy all the fields.
     * So instead of following where only the `id` field is updated,
     * <pre>
     *     GenericRecord modifiedGenericRecord = genericRecord.newBuilder()
     *                         .writeUTF("name", genericRecord.readUTF("name"))
     *                         .writeLong("id", 4)
     *                         .writeUTF("surname", genericRecord.readUTF("surname"))
     *                         .writeInt("age", genericRecord.readInt("age")).build();
     * </pre>
     * `cloneWithBuilder` used as follows:
     * <pre>
     *     GenericRecord modifiedGenericRecord = genericRecord.cloneWithBuilder().writeInt("id", 4).build();
     * </pre>
     *
     * @return a generic record builder with same class definition as this one and populated with same values.
     */
    @Nonnull
    Builder cloneWithBuilder();

    /**
     * @param fieldName the name of the field
     * @return field type for the given field name
     * @throws IllegalArgumentException if the field name does not exist in the class definition
     */
    @Nonnull
    FieldType getFieldType(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return true if field exists in the definition of the class. Note that returns true even if the field is null.
     */
    boolean hasField(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    boolean readBoolean(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    byte readByte(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    char readChar(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    double readDouble(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    float readFloat(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    int readInt(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    long readLong(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    short readShort(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    String readUTF(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    GenericRecord readGenericRecord(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    boolean[] readBooleanArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    byte[] readByteArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    char[] readCharArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    double[] readDoubleArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    float[] readFloatArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    int[] readIntArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    long[] readLongArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    short[] readShortArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    String[] readUTFArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    GenericRecord[] readGenericRecordArray(@Nonnull String fieldName);

    /**
     * Interface for creating {@link GenericRecord} instances.
     */
    @Beta
    interface Builder {

        /**
         * Creates a Builder that will build a {@link GenericRecord} in {@link Portable} format with a new class definition:
         * <pre>
         *     ClassDefinition classDefinition =
         *                 new ClassDefinitionBuilder(FACTORY_ID, CLASS_ID)
         *                         .addUTFField("name").addIntField("id").build();
         *
         *     GenericRecord genericRecord = GenericRecord.Builder.portable(classDefinition)
         *           .writeUTF("name", "foo")
         *           .writeInt("id", 123).build();
         * </pre>
         *
         * @param classDefinition of the portable that we will create
         * @return GenericRecordBuilder for Portable format
         */
        @Nonnull
        static Builder portable(@Nonnull ClassDefinition classDefinition) {
            return new PortableGenericRecordBuilder(classDefinition);
        }

        /**
         * @return a new constructed GenericRecord
         * @throws HazelcastSerializationException when the GenericRecord cannot be build.
         */
        @Nonnull
        GenericRecord build();

        /**
         * It is legal to overwrite the field once only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
         * Otherwise, it is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  It should be composed of only alpha-numeric characters.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @return itself for chaining
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeBoolean(@Nonnull String fieldName, boolean value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  It should be composed of only alpha-numeric characters.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeByte(@Nonnull String fieldName, byte value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeChar(@Nonnull String fieldName, char value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeDouble(@Nonnull String fieldName, double value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeFloat(@Nonnull String fieldName, float value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeInt(@Nonnull String fieldName, int value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeLong(@Nonnull String fieldName, long value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeShort(@Nonnull String fieldName, short value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeUTF(@Nonnull String fieldName, @Nullable String value);

        /**
         * It is illegal to write to the same field twice.
         * This method allows nested structures. Subclass should also created as `GenericRecord`
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeBooleanArray(@Nonnull String fieldName, @Nullable boolean[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeByteArray(@Nonnull String fieldName, @Nullable byte[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeCharArray(@Nonnull String fieldName, @Nullable char[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeFloatArray(@Nonnull String fieldName, @Nullable float[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeIntArray(@Nonnull String fieldName, @Nullable int[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeDoubleArray(@Nonnull String fieldName, @Nullable double[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeLongArray(@Nonnull String fieldName, @Nullable long[] value);

        /**
         * It is illegal to write to the same field twice.
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeShortArray(@Nonnull String fieldName, @Nullable short[] value);

        /**
         * It is illegal to write to the same field twice.
         * <p>
         * Array items can not be null
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeUTFArray(@Nonnull String fieldName, @Nullable String[] value);

        /**
         * It is illegal to write to the same field twice.
         * This method allows nested structures. Subclasses should also created as `GenericRecord`
         * <p>
         * Array items can not be null
         *
         * @param fieldName name of the field as it is defined in its class definition.
         *                  See {@link ClassDefinition} for {@link Portable}
         * @param value
         * @throws HazelcastSerializationException if the field name does not exist in the class definition or
         *                                         the type of the field does not match the one in the class definition or
         *                                         Same field is trying to be overwritten without using
         *                                         {@link GenericRecord#cloneWithBuilder()}.
         */
        Builder writeGenericRecordArray(@Nonnull String fieldName, @Nullable GenericRecord[] value);
    }
}
