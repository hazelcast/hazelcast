package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

/**
 * User: ahmetmircik
 * Date: 10/31/13
 */
public class NearCacheClearOperation extends AbstractOperation {
    MapService mapService;
    String mapName;

    public NearCacheClearOperation() {
    }

    public NearCacheClearOperation(String mapName) {
        this.mapName = mapName;
    }

    public void run() {
        mapService = getService();
        if(mapService.getMapContainer(mapName).isNearCacheEnabled())  {
            mapService.clearNearCache(mapName);
        }
        else {
            getLogger().warning("Cache clear operation has been accepted while near cache is not enabled for "+mapName+" map. Possible configuration conflict among nodes.");
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
    }

    @Override
    public String toString() {
        return "NearCacheClearOperation{}";
    }

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
