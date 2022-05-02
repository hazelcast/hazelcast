package com.hazelcast.tpc.requestservice;

public class OpCodes {

    public final static byte TABLE_UPSERT = 0;
    public final static byte TABLE_SELECT_BY_KEY = 1;
    public final static byte NOOP = 2;
    public final static byte RANDOM_LOAD = 3;
    public final static byte GET = 4;
    public final static byte SET = 5;
    public final static byte QUERY = 6;

    public final static byte INIT_BULK_TRANSPORT = 7;
    public final static byte BULK_TRANSPORT = 8;

    public final static byte MAX_OPCODE = BULK_TRANSPORT;
}
