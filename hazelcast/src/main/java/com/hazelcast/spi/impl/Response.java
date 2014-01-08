package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * A {@link com.hazelcast.spi.impl.Response} is a result of an {@link com.hazelcast.spi.Operation} being executed.
 * There are different types of responses:
 * <ol>
 *     <li>
 *          {@link com.hazelcast.spi.impl.NormalResponse} the result of a regular Operation result, e.g. Map.put
 *     </li>
 *     <li>
 *          {@link com.hazelcast.spi.impl.BackupResponse} the result of a completed {@link com.hazelcast.spi.impl.Backup}.
 *     </li>
 * </ol>
 */
abstract public class Response implements IdentifiedDataSerializable {

    protected long callId;
    protected boolean urgent;

    public Response() {
    }

    public Response(long callId,boolean urgent) {
        this.callId = callId;
        this.urgent = urgent;
    }

    /**
     * Check if this Response is an urgent response.
     *
     * @return true if urgent, false otherwise.
     */
    public boolean isUrgent() {
        return urgent;
    }

    /**
     * Returns the call id of the operation this response belongs to.
     *
     * @return the call id.
     */
    public long getCallId() {
        return callId;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(callId);
        out.writeBoolean(urgent);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        callId = in.readLong();
        urgent = in.readBoolean();
    }
}
