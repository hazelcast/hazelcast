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

package com.hazelcast.jet.memory.api.binarystorage.oalayout;

import com.hazelcast.nio.Bits;

public interface RecordHeaderLayOut {
    int HEADER_SIZE_BYTES = 35;

    int HEADERS_DELTA = HEADER_SIZE_BYTES - Bits.LONG_SIZE_IN_BYTES - Bits.SHORT_SIZE_IN_BYTES;

    // 2 bytes
    int SOURCE_OFFSET = 0;

    // 8 bytes
    int NEXT_RECORD_OFFSET = 2;

    // 8 bytes
    int LAST_RECORD_OFFSET = 10;

    // 8 bytes
    int RECORDS_COUNT_OFFSET = 18;

    // 8 bytes
    int HASH_CODE_OFFSET = 26;

    // 1 byte
    int MARKER_OFFSET = 34;
}
