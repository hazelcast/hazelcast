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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The {@link ConcurrentSkipListMap} serializer
 */
public class ConcurrentSkipListMapStreamSerializer extends AbstractMapStreamSerializer {
    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_CONCURRENT_SKIP_LIST_MAP;
    }

    @Override
    protected Map createMap(ObjectDataInput in, int size)
            throws IOException {
        Comparator comparator = in.readObject();
        return new ConcurrentSkipListMap(comparator);
    }

    @Override
    protected void beforeSerializeEntries(ObjectDataOutput out, Map map)
            throws IOException {
        ConcurrentSkipListMap concurrentSkipListMap = (ConcurrentSkipListMap) map;
        Comparator comparator = concurrentSkipListMap.comparator();
        out.writeObject(comparator);
    }
}
