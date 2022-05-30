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

package com.hazelcast.tpc.engine.iouring;

import io.netty.channel.unix.Buffer;
import io.netty.incubator.channel.uring.Native;

import java.nio.ByteBuffer;

class AcceptMemory {
    final ByteBuffer memory;
    final long memoryAddress;
    final ByteBuffer lengthMemory;
    final long lengthMemoryAddress;

    AcceptMemory() {
        this.memory = Buffer.allocateDirectWithNativeOrder(Native.SIZEOF_SOCKADDR_STORAGE);
        this.memoryAddress = Buffer.memoryAddress(memory);
        this.lengthMemory = Buffer.allocateDirectWithNativeOrder(Long.BYTES);
        // Needs to be initialized to the size of acceptedAddressMemory.
        // See https://man7.org/linux/man-pages/man2/accept.2.html
        this.lengthMemory.putLong(0, Native.SIZEOF_SOCKADDR_STORAGE);
        this.lengthMemoryAddress = Buffer.memoryAddress(lengthMemory);
    }
}
