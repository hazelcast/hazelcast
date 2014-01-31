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

package com.hazelcast.mapreduce;

import com.hazelcast.spi.annotation.Beta;

/**
 * This interface can be used to mark implementation being aware of the data partition
 * it is currently working on.
 *
 * @since 3.2
 */
@Beta
public interface PartitionIdAware {

    /**
     * Sets the partition id the implementing instance is executed against
     *
     * @param partitionId current partitionId
     */
    void setPartitionId(int partitionId);

}
