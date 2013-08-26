package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.Node")
public class NodeMBean extends HazelcastMBean<Node> {

    public NodeMBean(HazelcastInstance hazelcastInstance, Node node, ManagementService service) {
        super(node, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.Node"));
        properties.put("name", quote("node"+node.address));
        properties.put("HazelcastInstance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("address")
    @ManagedDescription("Address of the node")
    public String getName() {
        return managedObject.address.toString();
    }

    @ManagedAnnotation("masterAddress")
    @ManagedDescription("The master address of the cluster")
    public String getMasterAddress() {
        Address a =  managedObject.getMasterAddress();
        return a == null?null:a.toString();
    }
}
