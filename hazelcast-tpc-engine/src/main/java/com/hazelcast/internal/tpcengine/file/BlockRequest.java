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

package com.hazelcast.internal.tpcengine.file;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.IntPromise;

@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class BlockRequest {
    public static final int BLK_REQ_OP_NOP = 1;
    public static final int BLK_REQ_OP_READ = 2;
    public static final int BLK_REQ_OP_WRITE = 3;
    public static final int BLK_REQ_OP_FSYNC = 4;
    public static final int BLK_REQ_OP_FDATASYNC = 5;
    public static final int BLK_REQ_OP_OPEN = 6;
    public static final int BLK_REQ_OP_CLOSE = 7;
    public static final int BLK_REQ_OP_FALLOCATE = 8;

    public IOBuffer buf;
    public AsyncFile file;
    public long offset;
    public int len;
    public byte opcode;
    public int flags;
    public int rwFlags;
    public long addr;
    public IntPromise promise;
}
