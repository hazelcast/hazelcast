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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.QueryException;

@FunctionalInterface
public interface UpsertInjector {

    UpsertInjector FAILING_TOP_LEVEL_INJECTOR =
            value -> {
                if (value != null) {
                    throw QueryException.error("Writing to top-level fields not supported");
                }
            };

    void set(Object value);
}
