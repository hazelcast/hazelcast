/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.UUID;

public final class ProxyInfo {
    private final String serviceName;
    private final String objectName;
    private final UUID source;

    public ProxyInfo(String serviceName, String objectName, UUID source) {
        this.serviceName = serviceName;
        this.objectName = objectName;
        this.source = source;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getObjectName() {
        return objectName;
    }

    public UUID getSource() {
        return source;
    }

    @Override
    public String toString() {
        return "ProxyInfo{" + "serviceName='" + serviceName + '\'' + ", objectName='" + objectName + '\'' + ", source=" + source
                + '}';
    }
}
