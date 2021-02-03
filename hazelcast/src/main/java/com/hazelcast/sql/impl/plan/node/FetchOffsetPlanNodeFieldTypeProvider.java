/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

public final class FetchOffsetPlanNodeFieldTypeProvider implements PlanNodeFieldTypeProvider {

    public static final FetchOffsetPlanNodeFieldTypeProvider INSTANCE = new FetchOffsetPlanNodeFieldTypeProvider();

    private FetchOffsetPlanNodeFieldTypeProvider() {
        // No-op.
    }

    @Override
    public QueryDataType getType(int index) {
        throw QueryException.error("Columns cannot be referenced in LIMIT/OFFSET statements");
    }
}
