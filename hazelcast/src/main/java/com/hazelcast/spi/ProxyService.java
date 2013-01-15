/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.core.DistributedObject;

import java.util.Collection;

/**
 * @mdogan 1/14/13
 */
public interface ProxyService extends CoreService {
    ServiceProxy getProxy(String serviceName, Object proxyId);

    ServiceProxy getProxy(Class<? extends RemoteService> serviceClass, Object proxyId);

    void destroyProxy(String serviceName, Object proxyId);

    Collection<DistributedObject> getProxies(String serviceName);

    Collection<DistributedObject> getAllProxies();
}
