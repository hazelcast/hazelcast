/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.dto;

import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.json.JsonObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.util.JsonUtil.getObject;

/**
 * Holder class for serializable service beans.
 */
public class MXBeansDTO implements JsonSerializable {

    private EventServiceDTO eventServiceBean;
    private OperationServiceDTO operationServiceBean;
    private ConnectionManagerDTO connectionManagerBean;
    private PartitionServiceBeanDTO partitionServiceBean;
    private ProxyServiceDTO proxyServiceBean;
    private Map<String, ManagedExecutorDTO> managedExecutorBeans =
            new HashMap<String, ManagedExecutorDTO>();


    public MXBeansDTO() {
    }

    public EventServiceDTO getEventServiceBean() {
        return eventServiceBean;
    }

    public void setEventServiceBean(EventServiceDTO eventServiceBean) {
        this.eventServiceBean = eventServiceBean;
    }

    public OperationServiceDTO getOperationServiceBean() {
        return operationServiceBean;
    }

    public void setOperationServiceBean(OperationServiceDTO operationServiceBean) {
        this.operationServiceBean = operationServiceBean;
    }

    public ConnectionManagerDTO getConnectionManagerBean() {
        return connectionManagerBean;
    }

    public void setConnectionManagerBean(ConnectionManagerDTO connectionManagerBean) {
        this.connectionManagerBean = connectionManagerBean;
    }

    public PartitionServiceBeanDTO getPartitionServiceBean() {
        return partitionServiceBean;
    }

    public void setPartitionServiceBean(PartitionServiceBeanDTO partitionServiceBean) {
        this.partitionServiceBean = partitionServiceBean;
    }

    public ProxyServiceDTO getProxyServiceBean() {
        return proxyServiceBean;
    }

    public void setProxyServiceBean(ProxyServiceDTO proxyServiceBean) {
        this.proxyServiceBean = proxyServiceBean;
    }

    public ManagedExecutorDTO getManagedExecutorBean(String name) {
        return managedExecutorBeans.get(name);
    }

    public void putManagedExecutor(String name, ManagedExecutorDTO bean) {
        managedExecutorBeans.put(name, bean);
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        JsonObject managedExecutors = new JsonObject();
        for (Map.Entry<String, ManagedExecutorDTO> entry : managedExecutorBeans.entrySet()) {
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
            ManagedExecutorDTO managedExecutorBean = new ManagedExecutorDTO();
            managedExecutorBean.fromJson(next.getValue().asObject());
            managedExecutorBeans.put(next.getName(), managedExecutorBean);
        }
        eventServiceBean = new EventServiceDTO();
        eventServiceBean.fromJson(getObject(json, "eventServiceBean"));
        operationServiceBean = new OperationServiceDTO();
        operationServiceBean.fromJson(getObject(json, "operationServiceBean"));
        connectionManagerBean = new ConnectionManagerDTO();
        connectionManagerBean.fromJson(getObject(json, "connectionManagerBean"));
        proxyServiceBean = new ProxyServiceDTO();
        proxyServiceBean.fromJson(getObject(json, "proxyServiceBean"));
        partitionServiceBean = new PartitionServiceBeanDTO();
        partitionServiceBean.fromJson(getObject(json, "partitionServiceBean"));
    }
}
