/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

/**
 * @author mdogan 5/17/13
 */
public class ProxyFactoryConfig {

    private String service;

    private String className;

    public ProxyFactoryConfig() {
    }

    public ProxyFactoryConfig(String className, String service) {
        this.className = className;
        this.service = service;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    /*    private final Map<String, ClientProxyFactory> factoryMap = new HashMap<String, ClientProxyFactory>();

    public ProxyFactoryConfig addProxyFactory(String service, ClientProxyFactory factory) {
        factoryMap.put(service, factory);
        return this;
    }

    public Map<String, ClientProxyFactory> getFactories() {
        return factoryMap;
    }
    */
}
