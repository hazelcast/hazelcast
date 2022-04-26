package com.hazelcast.spi.impl.requestservice;

public class OpCodes {

    public final static byte TABLE_UPSERT = 0;
    public final static byte TABLE_SELECT_BY_KEY = 1;
    public final static byte NOOP = 2;
    public final static byte RANDOM_LOAD = 3;
    public final static byte GET = 4;
    public final static byte SET = 5;
    public final static byte QUERY = 6;
    public final static byte MAX_OPCODE = QUERY;
}
