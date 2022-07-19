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

package com.hazelcast.tpc.requestservice;



import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;

// always
// size: int
// flags: int
// partitionId: int  : 8
// request
// callid: long: 12
// opcode: 20
// response
// call id: long 12
public class FrameCodec {
    public static final int FLAG_OP = 1 << 1;
    public static final int FLAG_OP_RESPONSE = 1 << 2;
    public static final int FLAG_OP_RESPONSE_CONTROL = 1 << 3;
    public static final int RESPONSE_TYPE_OVERLOAD = 0;
    public static final int RESPONSE_TYPE_EXCEPTION = 1;
    public static final int OFFSET_SIZE = 0;
    public static final int OFFSET_FLAGS = OFFSET_SIZE + BYTES_INT;
    public static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + BYTES_INT;
    public static final int OFFSET_RES_CALL_ID = OFFSET_PARTITION_ID + BYTES_INT;
    public static final int OFFSET_RES_PAYLOAD = OFFSET_RES_CALL_ID + BYTES_LONG;
    public static final int OFFSET_REQ_CALL_ID = OFFSET_PARTITION_ID + BYTES_INT;
    public static final int OFFSET_REQ_OPCODE = OFFSET_REQ_CALL_ID + BYTES_LONG;
    public static final int OFFSET_REQ_PAYLOAD = OFFSET_REQ_OPCODE + BYTES_INT;
}
