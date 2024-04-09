/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.holder;

public class VectorPairHolder {
    /**
     * Marker name for unnamed vector. Empty string is not valid for vector index name.
     */
    public static final String SINGLE_VECTOR_NAME = "";

    public static final byte DENSE_FLOAT_VECTOR = 0;

    private final String name;
    private final byte type;
    private final float[] vector;

    public VectorPairHolder(String name, byte type, float[] vector) {
        this.name = name;
        this.type = type;
        this.vector = vector;
    }

    public String getName() {
        return name;
    }

    public byte getType() {
        return type;
    }

    public float[] getVector() {
        return vector;
    }
}
