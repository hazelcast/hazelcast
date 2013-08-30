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

package com.hazelcast.collections.list;

import com.hazelcast.collections.CollectionsContainer;
import com.hazelcast.collections.CollectionsService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 8/29/13
 */
public class ListService extends CollectionsService {

    public static final String SERVICE_NAME = "hz:impl:listService";

    private final ConcurrentMap<String, ListContainer> containerMap = new ConcurrentHashMap<String, ListContainer>();

    public ListService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    protected Map<String, ? extends CollectionsContainer> getContainerMap() {
        return containerMap;
    }

    protected String getServiceName() {
        return SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        //TODO create proxy
        return null;
    }

}
