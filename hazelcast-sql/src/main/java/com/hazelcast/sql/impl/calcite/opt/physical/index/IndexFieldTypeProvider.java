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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Specialized field type provider that do not expect any fields.
 */
public class IndexFieldTypeProvider implements PlanNodeFieldTypeProvider {

    public static final IndexFieldTypeProvider INSTANCE = new IndexFieldTypeProvider();

    private IndexFieldTypeProvider() {
        // No-op.
    }

    @Override
    public QueryDataType getType(int index) {
        throw new IllegalStateException("The operation should not be called.");
    }
}
