/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Provides a mean of writing portable fields to a binary in form of java primitives
 * arrays of java primitives , nested portable fields and array of portable fields.
 */
public interface PortableWriter {

    /**
     * Writes a primitive int.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeInt(String fieldName, int value) throws IOException;

    /**
     * Writes a primitive long.
     *
     * @param fieldName name of the field
     * @param value     long value to be written
     * @throws IOException
     */
    void writeLong(String fieldName, long value) throws IOException;

    /**
     * Writes an UTF string.
     *
     * @param fieldName name of the field
     * @param value     utf string value to be written
     * @throws IOException
     */
    void writeUTF(String fieldName, String value) throws IOException;

    /**
     * Writes a primitive boolean.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeBoolean(String fieldName, boolean value) throws IOException;

    /**
     * Writes a primitive byte.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeByte(String fieldName, byte value) throws IOException;

    /**
     * Writes a primitive char.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeChar(String fieldName, int value) throws IOException;

    /**
     * Writes a primitive double.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeDouble(String fieldName, double value) throws IOException;

    /**
     * Writes a primitive float.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeFloat(String fieldName, float value) throws IOException;

    /**
     * Writes a primitive short.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException
     */
    void writeShort(String fieldName, short value) throws IOException;

    /**
     * Writes a Portable.
     *
     * @param fieldName name of the field
     * @param portable  Portable to be written
     * @throws IOException
     */
    void writePortable(String fieldName, Portable portable) throws IOException;

    /**
     * To write a null portable value, user needs to provide class and factoryIds of related class.
     *
     * @param fieldName name of the field
     * @param factoryId factory id of related portable class
     * @param classId   class id of related portable class
     * @throws IOException
     */
    void writeNullPortable(String fieldName, int factoryId, int classId) throws IOException;

    /**
     * Writes a primitive byte-array.
     *
     * @param fieldName name of the field
     * @param bytes     byte array to be written
     * @throws IOException
     */
    void writeByteArray(String fieldName, byte[] bytes) throws IOException;

    /**
     * Writes a primitive char-array.
     *
     * @param fieldName name of the field
     * @param chars     char array to be written
     * @throws IOException
     */
    void writeCharArray(String fieldName, char[] chars) throws IOException;

    /**
     * Writes a primitive int-array.
     *
     * @param fieldName name of the field
     * @param ints      int array to be written
     * @throws IOException
     */
    void writeIntArray(String fieldName, int[] ints) throws IOException;

    /**
     * Writes a primitive long-array.
     *
     * @param fieldName name of the field
     * @param longs     long array to be written
     * @throws IOException
     */
    void writeLongArray(String fieldName, long[] longs) throws IOException;

    /**
     * Writes a primitive double array.
     *
     * @param fieldName name of the field
     * @param values    double array to be written
     * @throws IOException
     */
    void writeDoubleArray(String fieldName, double[] values) throws IOException;

    /**
     * Writes a primitive float array.
     *
     * @param fieldName name of the field
     * @param values    float array to be written
     * @throws IOException
     */
    void writeFloatArray(String fieldName, float[] values) throws IOException;

    /**
     * Writes a primitive short-array.
     *
     * @param fieldName name of the field
     * @param values    short array to be written
     * @throws IOException
     */
    void writeShortArray(String fieldName, short[] values) throws IOException;

    /**
     * Writes a an array of Portables.
     *
     * @param fieldName name of the field
     * @param portables portable array to be written
     * @throws IOException
     */
    void writePortableArray(String fieldName, Portable[] portables) throws IOException;

    /**
     * After writing portable fields, one can write remaining fields in old fashioned way consecutively at the end
     * of stream. User should not that after getting raw dataoutput trying to write portable fields will result
     * in IOException
     *
     * @return ObjectDataOutput
     * @throws IOException
     */
    ObjectDataOutput getRawDataOutput() throws IOException;
}
