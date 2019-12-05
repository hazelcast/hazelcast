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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.proxyservice.ProxyService;

import java.util.Map;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link ProxyService}
 */
@ManagedDescription("HazelcastInstance.ProxyService")
public class ProxyServiceMBean extends HazelcastMBean<ProxyService> {

    private static final int INITIAL_CAPACITY = 3;

    public ProxyServiceMBean(HazelcastInstance hazelcastInstance, ProxyService proxyService, ManagementService service) {
        super(proxyService, service);

        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
        properties.put("type", quote("HazelcastInstance.ProxyService"));
        properties.put("name", quote("proxyService" + hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("proxyCount")
    @ManagedDescription("The number proxies")
    public int getProxyCount() {
        return managedObject.getProxyCount();
    }
}
