package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionService;

import java.net.InetSocketAddress;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.PartitionServiceMBean")
public class PartitionServiceMBean  extends HazelcastMBean<PartitionService> {

    private final HazelcastInstanceImpl hazelcastInstance;

    public PartitionServiceMBean(HazelcastInstanceImpl hazelcastInstance, PartitionService partitionService, ManagementService service) {
        super(partitionService, service);

        this.hazelcastInstance = hazelcastInstance;
        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.PartitionServiceMBean"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("partitionCount")
    @ManagedDescription("Number of partitions")
    public int getPartitionCount() {
        return managedObject.getPartitionCount();
    }

    @ManagedAnnotation("activePartitionCount")
    @ManagedDescription("Number of active partitions")
    public int getActivePartitionCount() {
        InetSocketAddress address = hazelcastInstance.getCluster().getLocalMember().getInetSocketAddress();
        return managedObject.getMemberPartitions(new Address(address)).size();
    }
}
