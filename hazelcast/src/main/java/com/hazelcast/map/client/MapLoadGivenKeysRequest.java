package com.hazelcast.map.client;

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.MapLoadAllOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Loads all given keys from a map store.
 */
public class MapLoadGivenKeysRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

    private String name;

    private List<Data> keys;

    private boolean replaceExistingValues;

    public MapLoadGivenKeysRequest() {
        keys = Collections.emptyList();
    }

    public MapLoadGivenKeysRequest(String name, List<Data> keys, boolean replaceExistingValues) {
        this.name = name;
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MapLoadAllOperationFactory(name, keys, replaceExistingValues);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.LOAD_ALL_GIVEN_KEYS;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeBoolean("r", replaceExistingValues);
        final int size = keys.size();
        writer.writeInt("s", size);
        if (size > 0) {
            ObjectDataOutput output = writer.getRawDataOutput();
            for (Data key : keys) {
                key.writeData(output);
            }
        }
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        replaceExistingValues = reader.readBoolean("r");
        final int size = reader.readInt("s");
        if (size < 1) {
            keys = Collections.emptyList();
        } else {
            keys = new ArrayList<Data>(size);
            ObjectDataInput input = reader.getRawDataInput();
            for (int i = 0; i < size; i++) {
                Data key = new Data();
                key.readData(input);
                keys.add(key);
            }
        }

    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "loadAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{keys, replaceExistingValues};
    }
}
