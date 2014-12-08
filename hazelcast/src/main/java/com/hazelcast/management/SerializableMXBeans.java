/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.util.JsonUtil.getObject;

/**
 * Holder class for serializable service beans.
 */
public class SerializableMXBeans implements JsonSerializable {

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
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        JsonObject managedExecutors = new JsonObject();
        for (Map.Entry<String, SerializableManagedExecutorBean> entry : managedExecutorBeans.entrySet()) {
            managedExecutors.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("managedExecutorBeans", managedExecutors);
        root.add("eventServiceBean", eventServiceBean.toJson());
        root.add("operationServiceBean", operationServiceBean.toJson());
        root.add("connectionManagerBean", connectionManagerBean.toJson());
        root.add("partitionServiceBean", partitionServiceBean.toJson());
        root.add("proxyServiceBean", proxyServiceBean.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        final Iterator<JsonObject.Member> managedExecutorsIteartor = getObject(json, "managedExecutorBeans").iterator();
        while (managedExecutorsIteartor.hasNext()) {
            final JsonObject.Member next = managedExecutorsIteartor.next();
            SerializableManagedExecutorBean managedExecutorBean = new SerializableManagedExecutorBean();
            managedExecutorBean.fromJson(next.getValue().asObject());
            managedExecutorBeans.put(next.getName(), managedExecutorBean);
        }
        eventServiceBean = new SerializableEventServiceBean();
        eventServiceBean.fromJson(getObject(json, "eventServiceBean"));
        operationServiceBean = new SerializableOperationServiceBean();
        operationServiceBean.fromJson(getObject(json, "operationServiceBean"));
        connectionManagerBean = new SerializableConnectionManagerBean();
        connectionManagerBean.fromJson(getObject(json, "connectionManagerBean"));
        proxyServiceBean = new SerializableProxyServiceBean();
        proxyServiceBean.fromJson(getObject(json, "proxyServiceBean"));
        partitionServiceBean = new SerializablePartitionServiceBean();
        partitionServiceBean.fromJson(getObject(json, "partitionServiceBean"));
    }
}
