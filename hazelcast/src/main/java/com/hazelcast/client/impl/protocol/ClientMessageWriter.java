/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;

/**
 * Interface that is passed to ClientMessageGenerator#append
 */
public interface ClientMessageWriter {

    /**
     * Appends  a primitive int.
     *
     * @param value int value to be written
     */
    void appendInt(int value);

    /**
     * Appends  a primitive long.
     *
     * @param value long value to be written
     * @
     */
    void appendLong(long value);

    /**
     * Appends  an UTF string.
     *
     * @param value utf string value to be written
     * @
     */
    void appendUTF(String value);

    /**
     * Appends  a primitive boolean.
     *
     * @param value int value to be written
     * @
     */
    void appendBoolean(boolean value);

    /**
     * Appends  a primitive byte.
     *
     * @param value int value to be written
     * @
     */
    void appendByte(byte value);

    /**
     * Appends  a primitive char.
     *
     * @param value int value to be written
     * @
     */
    void appendChar(char value);

    /**
     * Appends  a primitive double.
     *
     * @param value int value to be written
     * @
     */
    void appendDouble(double value);

    /**
     * Appends  a primitive float.
     *
     * @param value int value to be written
     * @
     */
    void appendFloat(float value);

    /**
     * Appends  a primitive short.
     *
     * @param value int value to be written
     * @
     */
    void appendShort(short value);

    /**
     * Appends  a Portable.
     *
     * @param portable Portable to be written
     * @
     */
    void appendObject(Object portable);

    /**
     * To append a null portable value, user needs to provide class and factoryIds of related class.
     *
     * @param factoryId factory id of related portable class
     * @param classId   class id of related portable class
     * @
     */
    void appendNullPortable(int factoryId, int classId);

    /**
     * Appends  a primitive byte-array.
     *
     * @param bytes byte array to be written
     * @
     */
    void appendByteArray(byte[] bytes);

    /**
     * Appends  a primitive char-array.
     *
     * @param chars char array to be written
     * @
     */
    void appendCharArray(char[] chars);

    /**
     * Appends  a primitive int-array.
     *
     * @param ints int array to be written
     * @
     */
    void appendIntArray(int[] ints);

    /**
     * Appends  a primitive long-array.
     *
     * @param longs long array to be written
     * @
     */
    void appendLongArray(long[] longs);

    /**
     * Appends  a primitive double array.
     *
     * @param values double array to be written
     * @
     */
    void appendDoubleArray(double[] values);

    /**
     * Appends  a primitive float array.
     *
     * @param values float array to be written
     * @
     */
    void appendFloatArray(float[] values);

    /**
     * Appends  a primitive short-array.
     *
     * @param values short array to be written
     * @
     */
    void appendShortArray(short[] values);

    /**
     * Appends  a an array of Portables.
     *
     * @param portables portable array to be written
     * @
     */
    void appendPortableArray(Portable[] portables);

    void appendData(Data data);
}
