package com.hazelcast.jmx;

import com.hazelcast.instance.Node;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

@ManagedDescription("Node")
public class NodeMBean extends HazelcastMBean<Node> {

    public NodeMBean(Node node, ManagementService service) {
        super(node, service);
        objectName = service.createObjectName("Node", "Node-"+node.getThisAddress().getId());

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ConnectionManagerMBean nodeMBean = new ConnectionManagerMBean(node.connectionManager,service);
        try {
            mbs.registerMBean(nodeMBean, objectName);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @ManagedAnnotation("address")
    @ManagedDescription("Address of the node")
    public String getName() {
        return managedObject.address.toString();
    }
}
