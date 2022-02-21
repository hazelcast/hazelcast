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

package com.hazelcast.internal.util;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.Data;

/**
 * Converts any {@link Data} implementation to {@link HeapData}
 */
public final class ToHeapDataConverter {

    private ToHeapDataConverter() {
    }

    /**
     * Converts Data to HeapData. Useful for offheap conversion.
     *
     * @param data
     * @return the onheap representation of data. If data is null, null is returned.
     */
    public static Data toHeapData(Data data) {
        if (data == null || data instanceof HeapData) {
            return data;
        }
        return new HeapData(data.toByteArray());
    }
}
