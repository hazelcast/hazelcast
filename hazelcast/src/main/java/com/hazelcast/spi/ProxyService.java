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

package com.hazelcast.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;

import java.util.Collection;

/**
 * A {@link com.hazelcast.spi.CoreService} responsible for managing the DistributedObject proxies.
 *
 * @author mdogan 1/14/13
 */
public interface ProxyService extends CoreService {

    int getProxyCount();

    void initializeDistributedObject(String serviceName, String objectId);

    DistributedObject getDistributedObject(String serviceName, String objectId);

    void destroyDistributedObject(String serviceName, String objectId);

    Collection<DistributedObject> getDistributedObjects(String serviceName);

    Collection<String> getDistributedObjectNames(String serviceName);

    Collection<DistributedObject> getAllDistributedObjects();

    String addProxyListener(DistributedObjectListener distributedObjectListener);

    boolean removeProxyListener(String registrationId);
}
