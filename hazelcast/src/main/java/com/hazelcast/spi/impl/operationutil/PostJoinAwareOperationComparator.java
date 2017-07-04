/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationutil;

import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PostJoinAwareOperation;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Sorts {@link PostJoinAwareOperation}s by sequence number.
 */
@SerializableByConvention
public class PostJoinAwareOperationComparator<T extends Operation & PostJoinAwareOperation>
        implements Comparator<T>, Serializable {

    @Override
    public int compare(T o1, T o2) {
        if (o1.equals(o2)) {
            return 0;
        }

        // since the two operations are not equal, never return equality even when their sequence numbers collide
        return o1.getSequence() < o2.getSequence() ? -1
                : 1;
    }
}
