package com.hazelcast.spi.impl.operationservice.impl.responses;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import java.io.IOException;

public class ErrorResponse extends Response {
    private Throwable cause;

    public ErrorResponse() {
    }

    public ErrorResponse(Throwable cause, long callId, boolean urgent) {
        super(callId, urgent);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.ERROR_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(cause);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        cause = in.readObject();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ErrorResponse");
        sb.append("{callId=").append(callId);
        sb.append(", urgent=").append(urgent);
        sb.append(", cause=").append(cause);
        sb.append('}');
        return sb.toString();
    }
}
