package com.hazelcast.tpc.requestservice;

// always
// size: int
// flags: int
// partitionId: int  : 8

// request
// callid: long: 12
// opcode: 20

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;

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
