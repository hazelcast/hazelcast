package com.hazelcast.map.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * Evict all request used by clients.
 */
public class MapEvictAllRequest extends InvocationClientRequest {

    private String name;

    public MapEvictAllRequest() {
    }

    public MapEvictAllRequest(String name) {
        this.name = name;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EVICT_ALL;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }


    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    protected void invoke() {
        setSingleConnection();
        final MapService mapService = getService();
        final DistributedObject distributedObject
                = mapService.getNodeEngine().getProxyService().getDistributedObject(MapService.SERVICE_NAME, name);
        final MapProxyImpl mapProxy = (MapProxyImpl) distributedObject;
        mapProxy.evictAll();
        final ClientEndpoint endpoint = getEndpoint();
        endpoint.sendResponse(Boolean.TRUE, getCallId());
    }
}
