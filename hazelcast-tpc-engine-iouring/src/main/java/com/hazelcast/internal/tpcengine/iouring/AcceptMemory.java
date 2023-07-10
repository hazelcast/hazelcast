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

package com.hazelcast.internal.tpcengine.iouring;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_SOCKADDR_STORAGE;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;

// There will only be 1 accept request at any given moment in the system
// So we don't need to worry about concurrent access to the same AcceptMemory.
@SuppressWarnings("checkstyle:VisibilityModifier")
public class AcceptMemory {
    public final ByteBuffer buffer;
    public final long addr;
    public final ByteBuffer lenBuffer;
    public final long lenAddr;

    public AcceptMemory() {
        this.buffer = ByteBuffer.allocateDirect(SIZEOF_SOCKADDR_STORAGE);
        buffer.order(ByteOrder.nativeOrder());
        this.addr = addressOf(buffer);

        this.lenBuffer = ByteBuffer.allocateDirect(SIZEOF_LONG);
        lenBuffer.order(ByteOrder.nativeOrder());

        // Needs to be initialized to the size of acceptedAddressMemory.
        // See https://man7.org/linux/man-pages/man2/accept.2.html
        this.lenBuffer.putLong(0, SIZEOF_SOCKADDR_STORAGE);
        this.lenAddr = addressOf(lenBuffer);
    }
}
