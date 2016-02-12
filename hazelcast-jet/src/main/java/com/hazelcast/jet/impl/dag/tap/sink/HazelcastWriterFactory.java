/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkTap;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.dag.tap.TapType;
import com.hazelcast.jet.spi.data.DataWriter;

public final class HazelcastWriterFactory {
    private HazelcastWriterFactory() {

    }

    public static DataWriter getWriter(TapType tapType,
                                       String name,
                                       SinkTapWriteStrategy sinkTapWriteStrategy,
                                       ContainerDescriptor containerDescriptor,
                                       int partitionId,
                                       SinkTap tap) {
        switch (tapType) {
            case HAZELCAST_LIST:
                return new HazelcastListPartitionWriter(containerDescriptor, sinkTapWriteStrategy, name);
            case HAZELCAST_MAP:
                return new HazelcastMapPartitionWriter(containerDescriptor, partitionId, sinkTapWriteStrategy, name);
            case HAZELCAST_MULTIMAP:
                return new HazelcastMultiMapPartitionWriter(containerDescriptor, partitionId, sinkTapWriteStrategy, name);
            case FILE:
                return new DataFileWriter(containerDescriptor, partitionId, tap);
            default:
                throw new IllegalStateException("Unknown tuple type: " + tapType);
        }
    }
}
