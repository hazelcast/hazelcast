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

package com.hazelcast.cp.internal.datastructures.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;

/**
 * Creates and destroys CP data structure instances and their proxies
 */
public interface RaftRemoteService {

    /**
     * Creates a proxy for a CP data structure.
     * This method is called outside of the Raft layer.
     *
     * @param objectName the name of the CP data structure
     * @return the created proxy
     */
    <T extends DistributedObject> T createProxy(String objectName);

    /**
     * Destroys the given CP data structure on the CP group.
     * This operation is committed on the given CP group.
     */
    boolean destroyRaftObject(CPGroupId groupId, String objectName);
}
