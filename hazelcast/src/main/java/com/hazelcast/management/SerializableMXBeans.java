package com.hazelcast.management;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SerializableMXBeans implements DataSerializable{

    private SerializableEventServiceBean eventServiceBean;
    private SerializableOperationServiceBean operationServiceBean;
    private SerializableConnectionManagerBean connectionManagerBean;
    private SerializablePartitionServiceBean partitionServiceBean;
    private SerializableProxyServiceBean proxyServiceBean;
    private Map<String, SerializableManagedExecutorBean> managedExecutorBeans =
            new HashMap<String, SerializableManagedExecutorBean>();


    public SerializableMXBeans() {
    }

    public SerializableEventServiceBean getEventServiceBean() {
        return eventServiceBean;
    }

    public void setEventServiceBean(SerializableEventServiceBean eventServiceBean) {
        this.eventServiceBean = eventServiceBean;
    }

    public SerializableOperationServiceBean getOperationServiceBean() {
        return operationServiceBean;
    }

    public void setOperationServiceBean(SerializableOperationServiceBean operationServiceBean) {
        this.operationServiceBean = operationServiceBean;
    }

    public SerializableConnectionManagerBean getConnectionManagerBean() {
        return connectionManagerBean;
    }

    public void setConnectionManagerBean(SerializableConnectionManagerBean connectionManagerBean) {
        this.connectionManagerBean = connectionManagerBean;
    }

    public SerializablePartitionServiceBean getPartitionServiceBean() {
        return partitionServiceBean;
    }

    public void setPartitionServiceBean(SerializablePartitionServiceBean partitionServiceBean) {
        this.partitionServiceBean = partitionServiceBean;
    }

    public SerializableProxyServiceBean getProxyServiceBean() {
        return proxyServiceBean;
    }

    public void setProxyServiceBean(SerializableProxyServiceBean proxyServiceBean) {
        this.proxyServiceBean = proxyServiceBean;
    }

    public SerializableManagedExecutorBean getManagedExecutorBean(String name) {
        return managedExecutorBeans.get(name);
    }

    public void putManagedExecutor(String name, SerializableManagedExecutorBean bean) {
        managedExecutorBeans.put(name, bean);
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(managedExecutorBeans.size());
        for (Map.Entry<String, SerializableManagedExecutorBean> entry : managedExecutorBeans.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        eventServiceBean.writeData(out);
        operationServiceBean.writeData(out);
        connectionManagerBean.writeData(out);
        partitionServiceBean.writeData(out);
        proxyServiceBean.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
            SerializableManagedExecutorBean managedExecutorBean = new SerializableManagedExecutorBean();
            managedExecutorBean.readData(in);
            managedExecutorBeans.put(name, managedExecutorBean);
        }
        eventServiceBean = new SerializableEventServiceBean();
        eventServiceBean.readData(in);
        operationServiceBean = new SerializableOperationServiceBean();
        operationServiceBean.readData(in);
        connectionManagerBean = new SerializableConnectionManagerBean();
        connectionManagerBean.readData(in);
        partitionServiceBean = new SerializablePartitionServiceBean();
        partitionServiceBean.readData(in);
        proxyServiceBean = new SerializableProxyServiceBean();
        proxyServiceBean.readData(in);
    }
}
