/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.StringUtil;

import java.util.Comparator;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Comparator Provider for any kind of user supplied ({@link java.util.Comparator}).
 */
public final class QueueComparatorProvider {

    private QueueComparatorProvider() {
    }

    /**
     * @param queueConfig {@link QueueConfig} for
     *                    requested {@link Comparator}'s class name
     * @param classLoader the {@link ClassLoader} to be
     *                    used while creating custom {@link Comparator} from the supplied class name
     * @return {@link Comparator} instance if it is defined, otherwise
     * returns null to indicate there is no comparator defined
     */
    public static Comparator getPriorityQueueComparator(QueueConfig queueConfig,
                                                                                ClassLoader classLoader) {
        String priorityQueueComparatorClassName = queueConfig.getComparatorClassName();
        if (!isNullOrEmpty(priorityQueueComparatorClassName)) {
            try {
                return ClassLoaderUtil.newInstance(classLoader, priorityQueueComparatorClassName);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
        return null;
    }

    static Comparator createPriorityQueueComparator(String fullyQualifiedComparatorName) {
        if (!StringUtil.isNullOrEmpty(fullyQualifiedComparatorName)) {
            try {
                return ClassLoaderUtil.newInstance(null, fullyQualifiedComparatorName);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
        return null;
    }
}
