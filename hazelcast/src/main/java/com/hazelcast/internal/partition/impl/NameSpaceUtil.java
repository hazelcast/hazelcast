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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.MapUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Helper class for retrieving ServiceNamespace objects.
 */
public final class NameSpaceUtil {

    private NameSpaceUtil() {
    }

    /**
     * @param containers      collection of all containers in a partition
     * @param containerFilter allows only matching containers
     * @param toNamespace     returns {@link ObjectNamespace} for a container
     *
     * @return  a mutable collection of all service namespaces after functions are applied
     *          or an immutable empty collection, when no containers match the given predicate
     */
    public static <T> Collection<ServiceNamespace> getAllNamespaces(Map<?, T> containers,
                                                                    Predicate<T> containerFilter,
                                                                    Function<T, ObjectNamespace> toNamespace) {
        if (MapUtil.isNullOrEmpty(containers)) {
            return Collections.emptySet();
        }

        Collection<ServiceNamespace> collection = Collections.emptySet();
        for (T container : containers.values()) {
            if (!containerFilter.test(container)) {
                continue;
            }

            ObjectNamespace namespace = toNamespace.apply(container);

            if (collection.isEmpty()) {
                collection = new HashSet<>(collection);
            }

            collection.add(namespace);
        }

        return collection;
    }
}
