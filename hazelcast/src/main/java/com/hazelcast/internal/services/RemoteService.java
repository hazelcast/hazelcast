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

package com.hazelcast.internal.services;

import com.hazelcast.core.DistributedObject;

import java.util.UUID;

/**
 * An interface that can be implemented by SPI-Services to give them the ability to create proxies to
 * distributed objects.
 *
 * @author mdogan 10/31/12
 */
public interface RemoteService {

    /**
     * Creates a distributed object on the local member only (when {@code local} is
     * {@code true}), else cluster-wide.
     *
     * @param objectName the name for the created distributed object
     * @param source     The UUID of the client/member which initializes this request
     * @param local      when {@code true} then only perform required proxy creation
     *                   actions on the local member, otherwise perform cluster-wide
     *                   proxy creation.
     * @return the created distributed object
     */
    DistributedObject createDistributedObject(String objectName, UUID source, boolean local);

    /**
     * Destroys a distributed object.
     *
     * @param objectName the name of the distributed object to destroy
     */
    void destroyDistributedObject(String objectName, boolean local);

    /**
     * Creates a distributed object on the cluster.
     *
     * @param objectName the name for the created distributed object
     * @param source The UUID of the client/member which initializes this request
     * @return the created distributed object
     */
    default DistributedObject createDistributedObject(String objectName, UUID source) {
        return createDistributedObject(objectName, source, false);
    }

    /**
     * Destroys a distributed object on the cluster.
     *
     * @param objectName the name of the distributed object to destroy
     */
    default void destroyDistributedObject(String objectName) {
        destroyDistributedObject(objectName, false);
    }
}
