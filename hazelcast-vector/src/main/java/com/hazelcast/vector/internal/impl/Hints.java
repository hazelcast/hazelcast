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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.vector.Hint;

import javax.annotation.Nullable;

/**
 * Available {@link com.hazelcast.vector.SearchOptions} hints definitions.
 */
public class Hints {
    /**
     * Size of list of potential candidates during search. Similar to
     * {@link VectorIndexConfig#getEfConstruction()}.
     */
    public static final Hint<Integer> EF_SEARCH = new Hint<>("efSearch", Hints::parseNullableInt);

    /**
     * Number of results to fetch from partition. Applies both to 1-stage and 2-stage search.
     */
    public static final Hint<Integer> PARTITION_LIMIT = new Hint<>("partitionLimit", Hints::parseNullableInt);

    /**
     * Number of results to fetch from member in 2-stage search.
     * This hint does not apply to 1-stage search and to retries in case of errors/migrations
     * in 2-stage search.
     */
    public static final Hint<Integer> MEMBER_LIMIT = new Hint<>("memberLimit", Hints::parseNullableInt);

    /**
     * Force use of 1-stage search. If false, default rules of choosing search strategy apply.
     */
    public static final Hint<Boolean> FORCE_SINGLE_STAGE_SEARCH = new Hint<>("singleStage", Boolean::valueOf);

    private Hints() {
        // utility class
    }

    private static Integer parseNullableInt(@Nullable String value) {
        return value == null ? null : Integer.valueOf(value);
    }
}
