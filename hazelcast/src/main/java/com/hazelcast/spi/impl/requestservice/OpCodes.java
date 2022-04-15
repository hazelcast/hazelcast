package com.hazelcast.spi.impl.requestservice;

public class OpCodes {

    public final static byte TABLE_UPSERT = 0;
    public final static byte TABLE_SELECT_BY_KEY = 1;
    public final static byte TABLE_NOOP = 2;
    public final static byte RANDOM_LOAD = 3;
    public final static byte MAX_OPCODE = RANDOM_LOAD;
}
