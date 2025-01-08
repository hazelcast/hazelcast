/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl;

import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.vector.VectorValues;

import java.io.IOException;


/**
 * Utility class for handling I/O operations related to vector data.
 */
public class VectorIOUtils {

    private VectorIOUtils() {
    }

    /**
     * Writes the specified {@link VectorValues} to the given {@link ObjectDataOutput} stream.
     * <p>
     * This method is necessary due to the use of native memory for storing vector floats.
     * It enables direct copying to the output stream without loading the vector onto the heap.
     *
     * @param out  the {@link ObjectDataOutput} stream to which the vector values should be written.
     * @param data the {@link VectorValues} object containing the vector values to be written.
     * @throws IOException if an I/O error occurs during the write operation.
     */
    public static void writeSingleVectorValue(ObjectDataOutput out, VectorValues data) throws IOException {
        if (data == null) {
            out.writeInt(NULL_ARRAY_LENGTH);
            return;
        }
        DataSerializable serializable = (DataSerializable) data;
        serializable.writeData(out);
    }

    /**
     * Reads and deserializes {@link com.hazelcast.vector.VectorValues.SingleVectorValues}
     * from the specified {@link ObjectDataInput} stream.
     * <p>
     * This method reads a float array from the input stream and wraps it in a {@link SingleIndexVectorValues} object,
     * which is an on-heap implementation of {@link com.hazelcast.vector.VectorValues.SingleVectorValues}.
     * </p>
     *
     * @param in the {@link ObjectDataInput} stream from which the vector data should be read.
     * @return a {@link VectorValues} object containing the deserialized vector data.
     * @throws IOException if an I/O error occurs during the read operation.
     */
    public static VectorValues readSingleVectorValue(ObjectDataInput in) throws IOException {
        var vector = in.readFloatArray();
        if (vector == null) {
            return null;
        }
        return VectorValues.of(vector);
    }
}
