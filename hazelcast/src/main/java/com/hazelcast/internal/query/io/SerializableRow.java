package com.hazelcast.internal.query.io;

import com.hazelcast.nio.serialization.DataSerializable;

public interface SerializableRow extends Row, DataSerializable {
    // No-op.
}
