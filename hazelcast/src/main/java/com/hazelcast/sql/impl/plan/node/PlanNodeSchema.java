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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Schema of a node.
 */
public class PlanNodeSchema implements PlanNodeFieldTypeProvider {

    private final List<QueryDataType> types;
    private final int rowWidth;

    public PlanNodeSchema(List<QueryDataType> types) {
        assert types != null;

        this.types = Collections.unmodifiableList(types);

        rowWidth = calculateRowWidth(types);
    }

    public static PlanNodeSchema combine(PlanNodeSchema schema1, PlanNodeSchema schema2) {
        ArrayList<QueryDataType> types = new ArrayList<>(schema1.types);

        types.addAll(schema2.types);

        return new PlanNodeSchema(types);
    }

    @Override
    public QueryDataType getType(int index) {
        assert index <= types.size();

        return types.get(index);
    }

    public List<QueryDataType> getTypes() {
        return types;
    }

    public int getRowWidth() {
        return rowWidth;
    }

    private static int calculateRowWidth(List<QueryDataType> types) {
        int res = 0;

        for (QueryDataType type : types) {
            res += type.getTypeFamily().getEstimatedSize();
        }

        return res;
    }
}
