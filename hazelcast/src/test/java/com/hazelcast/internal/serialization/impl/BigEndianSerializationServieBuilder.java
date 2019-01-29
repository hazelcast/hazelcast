package com.hazelcast.internal.serialization.impl;

import java.nio.ByteOrder;

public class BigEndianSerializationServieBuilder extends DefaultSerializationServiceBuilder {
    @Override
    protected void overrideByteOrder() {
        byteOrder = ByteOrder.BIG_ENDIAN;
    }
}
