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

package com.hazelcast.multimap.operations;

import com.hazelcast.multimap.MultiMapContainer;
import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.multimap.MultiMapWrapper;
import com.hazelcast.nio.serialization.Data;

/**
 * @author ali 1/16/13
 */
public class CountOperation extends MultiMapKeyBasedOperation {

    public CountOperation() {
    }

    public CountOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        ((MultiMapService) getService()).getLocalMultiMapStatsImpl(name).incrementOtherOperations();
        MultiMapWrapper wrapper = container.getMultiMapWrapper(dataKey);
        response = wrapper == null ? 0 : wrapper.getCollection().size();
    }

    public int getId() {
        return MultiMapDataSerializerHook.COUNT;
    }

}
