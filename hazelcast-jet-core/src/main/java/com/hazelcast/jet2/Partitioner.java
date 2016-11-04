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

package com.hazelcast.jet2;

import com.hazelcast.spi.partition.IPartitionService;
import java.io.Serializable;

/**
 * Javadoc pending
 */
@FunctionalInterface
public interface Partitioner extends Serializable {

    default void init(IPartitionService service) {
    }

    /**
     * @return the partition for the given object
     */
    int getPartition(Object item, int numPartitions);

    class Default implements Partitioner {
        protected transient IPartitionService service;

        @Override
        public void init(IPartitionService service) {
            this.service = service;
        }

        @Override
        public int getPartition(Object item, int numPartitions) {
            return service.getPartitionId(item) % numPartitions;
        }
    }

    class DefaultWithExtractor extends Default {
        private KeyExtractor extractor;

        public DefaultWithExtractor(KeyExtractor extractor) {
            this.extractor = extractor;
        }

        @Override
        public int getPartition(Object item, int numPartitions) {
            return service.getPartitionId(extractor.extract(item)) % numPartitions;
        }
    }
}
