package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import java.net.InetSocketAddress;

import static com.hazelcast.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.PartitionServiceMBean}.
 */
public class SerializablePartitionServiceBean implements JsonSerializable {

    private int partitionCount;
    private int activePartitionCount;

    public SerializablePartitionServiceBean() {
    }

    public SerializablePartitionServiceBean(InternalPartitionService partitionService,
                                            HazelcastInstanceImpl hazelcastInstance) {
        InetSocketAddress address = hazelcastInstance.getCluster().getLocalMember().getSocketAddress();
        this.partitionCount = partitionService.getPartitionCount();
        this.activePartitionCount = partitionService.getMemberPartitions(new Address(address)).size();
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int getActivePartitionCount() {
        return activePartitionCount;
    }

    public void setActivePartitionCount(int activePartitionCount) {
        this.activePartitionCount = activePartitionCount;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("partitionCount", partitionCount);
        root.add("activePartitionCount", activePartitionCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        partitionCount = getInt(json, "partitionCount", -1);
        activePartitionCount = getInt(json, "activePartitionCount", -1);
    }
}
