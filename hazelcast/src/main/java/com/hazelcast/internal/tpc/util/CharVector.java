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

package com.hazelcast.internal.tpc.util;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

/**
 * A vector containing chars that is backed by an {@link IOBuffer}. Instead of
 * working with strings which are immutable and cause litter, the IOBuffer is
 * used.
 * <p>
 * todo: should have option to handle StringBuffers.
 */
public class CharVector {

    private IOBuffer buffer;
    // The position in the buffer containing the first characters of the string.
    private int startPos;
    private int length;

    public CharVector() {
    }

    public void deserialize(IOBuffer buffer) {
        this.buffer = buffer;
        this.length = buffer.readInt();
        this.startPos = buffer.position();

        buffer.incPosition(length);
    }

    public void clear() {
        buffer = null;
    }
}
