package com.hazelcast.map.operation;

import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.Set;

/**
 * User: ahmetmircik
 * Date: 10/31/13
 */
public class NearCacheKeySetInvalidationOperation extends AbstractOperation {
    MapService mapService;
    MapKeySet mapKeySet;
    String mapName;

    public NearCacheKeySetInvalidationOperation() {
    }

    public NearCacheKeySetInvalidationOperation(String mapName, Set<Data> keys) {
        this.mapKeySet = new MapKeySet(keys);
        this.mapName = mapName;
    }

    public void run() {
        mapService = getService();
        mapService.invalidateNearCache(mapName, mapKeySet.getKeySet());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        mapKeySet.writeData(out);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        mapKeySet = new MapKeySet();
        mapKeySet.readData(in);
    }

    @Override
    public String toString() {
        return "NearCacheKeySetInvalidationOperation{}";
    }

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
