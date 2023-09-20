/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.List;

/**
 * Visitor for {@link SqlNode} that collects indexes of query input parameters used in the SqlNode
 */
class ParamCollectingVisitor extends SqlBasicVisitor<SqlNode> {

    private final List<Integer> parameterPositions;

    ParamCollectingVisitor(List<Integer> parameterPositions) {
        this.parameterPositions = parameterPositions;
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        parameterPositions.add(param.getIndex());
        return param;
    }
}
