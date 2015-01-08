package com.hazelcast.spi.impl.operationservice.impl.responses;

import com.hazelcast.spi.impl.SpiDataSerializerHook;

/**
 * A response that is send to indicate that a 'fake' result is ready. This is needed for back pressure. This triggers the future
 * to unblock if needed.
 */
public class ForcedSyncResponse extends Response {

    public ForcedSyncResponse() {
    }

    public ForcedSyncResponse(long callId, boolean urgent) {
        super(callId, urgent);
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.FORCED_SYNC_RESPONSE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ForcedSyncResponse that = (ForcedSyncResponse) o;

        if (callId != that.callId) {
            return false;
        }
        if (urgent != that.urgent) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (callId ^ (callId >>> 32));
        result = 31 * result + (urgent ? 1 : 0);
        return result;
    }
}
