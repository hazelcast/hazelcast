package com.hazelcast.client.impl.client;

import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Portable;

import java.security.Permission;

public class GetSlotsRequest extends ClientRequest implements Portable {

    @Override
    public final void process() throws Exception {
        System.out.println("Calling GetSlotsRequest");
        Connection connection = getEndpoint().getConnection();
        int slots = clientEngine.getClaimAccounting().claimSlots(connection);
        endpoint.sendResponse(slots, getCallId());
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME; //TODO: Change me
    }

    @Override
    public int getFactoryId() {
        return BackpressurePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return BackpressurePortableHook.SLOT_REQUEST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
