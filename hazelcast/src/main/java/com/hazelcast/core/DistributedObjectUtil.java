/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Utility class for {@link DistributedObject}.
 */
public final class DistributedObjectUtil {

    private DistributedObjectUtil() {
    }

    /**
     * Gets the name of the given distributed object.
     *
     * @param distributedObject the {@link DistributedObject} instance whose name is requested
     * @return name of the given distributed object
     */
    public static String getName(DistributedObject distributedObject) {
        /*
         * The motivation of this behaviour is that some distributed objects (`ICache`) can have prefixed name.
         * For example, for the point of view of cache,
         * it has pure name and full name which contains prefixes also.
         *
         * However both of our `DistributedObject` and `javax.cache.Cache` (from JCache spec) interfaces
         * have same method name with same signature. It is `String getName()`.
         *
         * From the distributed object side, name must be fully qualified name of object,
         * but from the JCache spec side (also for backward compatibility),
         * it must be pure cache name without any prefix.
         * So there is same method name with same signature for different purposes.
         * Therefore, `PrefixedDistributedObject` has been introduced to retrieve the
         * fully qualified name of distributed object when it is needed.
         *
         * For cache case, the fully qualified name is full cache name contains Hazelcast prefix (`/hz`),
         * cache name prefix regarding to URI and/or classloader if specified and pure cache name.
         */
        if (distributedObject instanceof PrefixedDistributedObject) {
            return ((PrefixedDistributedObject) distributedObject).getPrefixedName();
        } else {
            return distributedObject.getName();
        }
    }
}
