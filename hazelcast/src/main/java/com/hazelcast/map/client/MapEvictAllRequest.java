package com.hazelcast.map.client;

import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.EvictAllOperationFactory;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.Map;

/**
 * Evict all request used by clients.
 */
public class MapEvictAllRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

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

    @Override
    protected OperationFactory createOperationFactory() {
        return new EvictAllOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int total = 0;
        MapService mapService = getService();
        for (Object result : map.values()) {
            Integer size = (Integer) mapService.toObject(result);
            total += size;
        }
        return total;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }
}
