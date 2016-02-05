package com.hazelcast.client.impl.protocol.codec;

public enum JetMessageType {

    JET_INIT(0x1a01),
    JET_SUBMIT(0x1a02),
    JET_EXECUTE(0x1a03),
    JET_INTERRUPT(0x1a04),
    JET_FINALIZEAPPLICATION(0x1a05),
    JET_LOCALIZE(0x1a06),
    JET_ACCEPTLOCALIZATION(0x1a07),
    JET_EVENT(0x1a08),
    JET_GETACCUMULATORS(0x1a09);

    private final int id;

    JetMessageType(int messageType) {
        this.id = messageType;
    }

    public int id() {
        return id;
    }


}


