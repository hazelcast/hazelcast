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

package com.hazelcast.core;

import java.util.EventListener;

/**
 * DistributedObjectListener notifies when a {@link DistributedObject}
 * is created or destroyed cluster-wide.
 *
 * @see DistributedObject
 * @see HazelcastInstance#addDistributedObjectListener(DistributedObjectListener)
 */
public interface DistributedObjectListener extends EventListener {

    /**
     * Invoked when a DistributedObject is created.
     *
     * @param event the event for the created DistributedObject
     */
    void distributedObjectCreated(DistributedObjectEvent event);

    /**
     * Invoked when a DistributedObject is destroyed.
     *
     * @param event the event for the destroyed DistributedObject
     */
    void distributedObjectDestroyed(DistributedObjectEvent event);
}
