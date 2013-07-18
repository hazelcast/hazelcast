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

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author mdogan 7/18/13
 */
public final class SerializationHelper {

    private final SerializationService serializationService;

    public SerializationHelper(HazelcastInstance instance) {
        serializationService = getNode(instance).getSerializationService();
    }

    public Data toData(final Object obj) {
        return serializationService.toData(obj);
    }

    private static Node getNode(HazelcastInstance hz) {
        final HazelcastInstanceImpl impl;
        if (hz instanceof HazelcastInstanceProxy) {
            impl = ((HazelcastInstanceProxy) hz).original;
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        } else {
            throw new IllegalArgumentException();
        }
        return impl.node;
    }
}
