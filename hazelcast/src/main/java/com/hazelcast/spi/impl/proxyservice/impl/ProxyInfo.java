/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.proxyservice.impl;

public final class ProxyInfo {
    private final String serviceName;
    private final String objectName;

    public ProxyInfo(String serviceName, String objectName) {
        this.serviceName = serviceName;
        this.objectName = objectName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProxyInfo{");
        sb.append("serviceName='").append(serviceName).append('\'');
        sb.append(", objectName='").append(objectName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
