/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.storage;

import javax.annotation.Nullable;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

public enum ConcurrentModificationPolicy {
    /**
     * Ignore race and return less than requested entries from partition.
     * This may be reconciled at higher level, as we usually query data from
     * many partitions so can use them to filling missing places. However, there would
     * still be an edge case, when the result aggregated from all partitions is smaller
     * than requested (in some use cases is might be fine).
     */
    SKIP {
        @Override
        void action(@Nullable Iterator<?> it) {
            if (it != null) {
                it.remove();
            }
        }
    },

    /**
     * If the entry was removed just after it was found, we may be unable to return metadata for it.
     * In such case we retry hoping that next time there will be no conflict. That way
     * the result is always correct and contains expected number of entries.
     */
    THROW {
        @Override
        void action(@Nullable Iterator<?> it) {
            throw new ConcurrentModificationException("Entry removed during search");
        }
    };

    /**
     * @param it iterator where the concurrent modification was detected
     */
    void action(@Nullable Iterator<?> it) {
        throw new UnsupportedOperationException("Not implemented");
    }

    void action() {
        action(null);
    }
}
