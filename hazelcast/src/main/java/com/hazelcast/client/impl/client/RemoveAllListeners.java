package com.hazelcast.client.impl.client;

import java.security.Permission;

/**
 * When a connection does not respond to heart-beat we switch the listeners to another endpoint
 * If somehow connection starts to respond heart-beat we need to signal the endpoint to remove the listeners
 */
public class RemoveAllListeners extends CallableClientRequest {

    public RemoveAllListeners() {
    }

    @Override
    public Object call() throws Exception {
        endpoint.clearAllListeners();
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.REMOVE_ALL_LISTENERS;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
