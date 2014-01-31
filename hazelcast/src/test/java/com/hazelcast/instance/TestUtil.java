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
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import org.junit.Ignore;

@Ignore("not a JUnit test")
public final class TestUtil {

    static final private SerializationService serializationService = new SerializationServiceBuilder().build();

    private TestUtil() {
    }

    public static Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    public static Object toObject(Data data) {
        return serializationService.toObject(data);
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            impl = ((HazelcastInstanceProxy) hz).original;
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    public static void terminateInstance(HazelcastInstance hz) {
        final Node node = getNode(hz);
        node.getConnectionManager().shutdown();
        node.shutdown(true);
    }

    public static void warmUpPartitions(HazelcastInstance...instances) throws InterruptedException {
        for (HazelcastInstance instance : instances) {
            final PartitionService ps = instance.getPartitionService();
            for (Partition partition : ps.getPartitions()) {
                while (partition.getOwner() == null) {
                    Thread.sleep(10);
                }
            }
        }
    }
}

