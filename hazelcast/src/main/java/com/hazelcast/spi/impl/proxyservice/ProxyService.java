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

package com.hazelcast.spi.impl.proxyservice;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.internal.services.CoreService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

/**
 * A {@link CoreService} responsible for managing the DistributedObject proxies.
 *
 * @author mdogan 1/14/13
 */
public interface ProxyService extends CoreService {

    int getProxyCount();

    void initializeDistributedObject(String serviceName, String objectId, UUID source);

    DistributedObject getDistributedObject(String serviceName, String objectId, UUID source);

    void destroyDistributedObject(String serviceName, String objectId, UUID source);

    Collection<DistributedObject> getDistributedObjects(String serviceName);

    Collection<String> getDistributedObjectNames(String serviceName);

    Collection<DistributedObject> getAllDistributedObjects();

    boolean existsDistributedObject(String serviceName, String objectId);

    /**
     * Returns the total number of created proxies for the given {@code serviceName},
     * even if some have already been destroyed.
     *
     * @param serviceName the distributed service name
     * @return the total count of created proxies
     */
    long getCreatedCount(@Nonnull String serviceName);

    UUID addProxyListener(DistributedObjectListener distributedObjectListener);

    boolean removeProxyListener(UUID registrationId);
}
