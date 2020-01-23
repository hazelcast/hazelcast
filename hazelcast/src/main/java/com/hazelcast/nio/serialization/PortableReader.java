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

import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.util.Set;

/**
 * Provides a mean of reading portable fields from a binary in form of java primitives
 * arrays of java primitives, nested portable fields and array of portable fields.
 * <p>
 * PortableReader read method family support nested paths. For example <code>body.brain.iq</code> is a valid nested path.
 */
public interface PortableReader {

    /**
     * @return version global version of portable classes
     */
    int getVersion();

    /**
     * @param fieldName name of the field (does not support nested paths)
     * @return true if field exist in this class.
     */
    boolean hasField(String fieldName);

    /**
     * @return set of field names on this portable class
     */
    Set<String> getFieldNames();

    /**
     * @param fieldName name of the field
     * @return field type of given fieldName
     */
    FieldType getFieldType(String fieldName);

    /**
     * @param fieldName name of the field
     * @return classId of given field
     */
    int getFieldClassId(String fieldName);

    /**
     * @param fieldName name of the field
     * @return the int value read
     * @throws IOException in case of any exceptional case
     */
    int readInt(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the long value read
     * @throws IOException in case of any exceptional case
     */
    long readLong(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the utf string value read
     * @throws IOException in case of any exceptional case
     */
    String readUTF(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the boolean value read
     * @throws IOException in case of any exceptional case
     */
    boolean readBoolean(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the byte value read
     * @throws IOException in case of any exceptional case
     */
    byte readByte(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the char value read
     * @throws IOException in case of any exceptional case
     */
    char readChar(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the double value read
     * @throws IOException in case of any exceptional case
     */
    double readDouble(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the float value read
     * @throws IOException in case of any exceptional case
     */
    float readFloat(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the short value read
     * @throws IOException in case of any exceptional case
     */
    short readShort(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @param <P> the type of the portable read
     * @return the portable value read
     * @throws IOException in case of any exceptional case
     */
    <P extends Portable> P readPortable(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the byte array value read
     * @throws IOException in case of any exceptional case
     */
    byte[] readByteArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the boolean array value read
     * @throws IOException in case of any exceptional case
     */
    boolean[] readBooleanArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the char array value read
     * @throws IOException in case of any exceptional case
     */
    char[] readCharArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the int array value read
     * @throws IOException in case of any exceptional case
     */
    int[] readIntArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the long array value read
     * @throws IOException in case of any exceptional case
     */
    long[] readLongArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the double array value read
     * @throws IOException in case of any exceptional case
     */
    double[] readDoubleArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the float array value read
     * @throws IOException in case of any exceptional case
     */
    float[] readFloatArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the short array value read
     * @throws IOException in case of any exceptional case
     */
    short[] readShortArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the String array value read
     * @throws IOException in case of any exceptional case
     */
    String[] readUTFArray(String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the portable value read
     * @throws IOException in case of any exceptional case
     */
    Portable[] readPortableArray(String fieldName) throws IOException;

    /**
     * {@link PortableWriter#getRawDataOutput()}.
     * <p>
     * Note that portable fields can not read after getRawDataInput() is called. In case this happens,
     * IOException will be thrown.
     *
     * @return rawDataInput
     * @throws IOException in case of any exceptional case
     */
    ObjectDataInput getRawDataInput() throws IOException;
}
