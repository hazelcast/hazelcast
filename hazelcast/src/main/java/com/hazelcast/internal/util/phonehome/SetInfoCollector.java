/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class SetInfoCollector implements MetricsCollector {

    Collection<DistributedObject> sets;

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {
        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();
        sets = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(SetService.SERVICE_NAME)).collect(toList());
        Map<String, String> setInfo = new HashMap<>();

        setInfo.put("sect", String.valueOf(sets.size()));

        return setInfo;
    }

}

