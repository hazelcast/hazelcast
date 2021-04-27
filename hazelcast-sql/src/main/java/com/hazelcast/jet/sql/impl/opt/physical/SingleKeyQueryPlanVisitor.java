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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.Serializable;
import java.util.List;

public class SingleKeyQueryPlanVisitor implements Serializable {
    private Expression<Boolean> filter;
    private HazelcastTable table;
    private boolean earlyExit;
    private List<Expression<?>> projection;

    public void onDelete(DeletePhysicalRel rel) {
        ((PhysicalRel) rel.getInput()).accept(this);
    }

    public void onScan(FullScanPhysicalRel rel) {
        table = rel.getTable().unwrap(HazelcastTable.class);
        filter = rel.filter(QueryParameterMetadata.EMPTY);
        projection = rel.projection(QueryParameterMetadata.EMPTY);
    }

    public void onValue(ValuesPhysicalRel rel) {
        earlyExit = true;
    }

    public boolean getEarlyExit() {
        return earlyExit;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }

    public HazelcastTable getTable() {
        return table;
    }

    public List<Expression<?>> getProjection() {
        return projection;
    }
}
