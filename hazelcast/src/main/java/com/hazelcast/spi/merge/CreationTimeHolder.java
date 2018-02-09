/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.merge;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * Represents a read-only view a creation time for the merging process after a split-brain.
 *
 * @since 3.10
 */
public interface CreationTimeHolder extends DataSerializable {

    /**
     * Returns the creation time of the merge data.
     *
     * @return the creation time of the merge data
     */
    long getCreationTime();
}
