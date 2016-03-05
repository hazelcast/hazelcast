/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.impl.binarystorage;

import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.jet.memory.impl.binarystorage.sorted.BinaryKeyValueOpenAddressingSortedStorage;

public final class BinaryStorageFactory {
    private BinaryStorageFactory() {
    }

    public static <T> BinaryKeyValueStorage<T> getStorage(
            BinaryComparator binaryComparator,
            HsaResizeListener hsaResizeListener
    ) {
        return new BinaryKeyValueOpenAddressingStorage<T>(
                null,
                binaryComparator.getHasher(),
                hsaResizeListener
        );
    }

    public static <T> BinaryKeyValueSortedStorage<T> getSortedStorage(
            BinaryComparator binaryComparator,
            HsaResizeListener hsaResizeListener) {
        return new BinaryKeyValueOpenAddressingSortedStorage<T>(
                null,
                binaryComparator,
                hsaResizeListener
        );
    }
}
