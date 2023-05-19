/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.util.HashUtil;

public final class CRC {

    private CRC() {
    }

    public static int crc32(byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    // todo: need to be replaced by a fast crc 32 implementation
    public static int crc32(byte[] bytes, int offset, int length) {
        return HashUtil.MurmurHash3_x86_32(bytes, 0, length);
    }
}
