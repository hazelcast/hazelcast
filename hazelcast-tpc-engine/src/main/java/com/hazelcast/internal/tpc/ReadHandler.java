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

package com.hazelcast.internal.tpc;

/**
 * The ReadHandler currently is a mess. The problem is that there is currently not a unified buffer
 * that fits with Nio (ByteBuffer) and IOUring (Netty ByteBuf). So we need to fix the
 * {@link com.hazelcast.internal.tpc.iobuffer.IOBuffer} so that we can have a single
 * ReadHandler that can be used for NioEventloop/IOUringEventLoop/EpollEventloop.
 */
public interface ReadHandler {
}
