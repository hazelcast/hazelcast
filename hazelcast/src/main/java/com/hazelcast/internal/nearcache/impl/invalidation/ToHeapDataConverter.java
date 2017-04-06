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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;

/**
 * Converts any {@link Data} implementation to {@link HeapData}
 */
public final class ToHeapDataConverter {

    private ToHeapDataConverter() {
    }

    public static Data toHeapData(Data key) {
        if (key instanceof HeapData) {
            return key;
        }
        return new HeapData(key.toByteArray());
    }
}
