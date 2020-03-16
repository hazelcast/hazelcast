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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.fragment.QueryFragmentContext;
import com.hazelcast.sql.impl.state.QueryState;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class SqlTestUtils {
    private SqlTestUtils() {
        // No-op.
    }

    public static QueryFragmentContext emptyFragmentContext() {
        return emptyFragmentContext(Collections.emptyList());
    }

    public static QueryFragmentContext emptyFragmentContext(List<Object> args) {
        QueryState state = QueryState.createInitiatorState(
            QueryId.create(UUID.randomUUID()),
            UUID.randomUUID(),
            null,
            0L,
            null,
            null,
            null
        );

        return new QueryFragmentContext(state, args, null, null);
    }
}
