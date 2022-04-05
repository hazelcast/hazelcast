/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.physical.index;

import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import org.apache.calcite.rex.RexNode;

/**
 * A candidate expression that could potentially be used to form a filter on some index component.
 * <p>
 * Consider the query {@code SELECT * FROM person WHERE name=? AND age=?}. After analysis two candidates would be
 * created: one for the {@code name=?} expression, and another for the {@code age=?} expression. If there is an index
 * on any of those columns, the engine will attempt to apply the candidate to the index to form an {@link IndexComponentFilter}.
 */
class IndexComponentCandidate {
    /** Original Calcite expression that formed this candidate. */
    private final RexNode expression;

    /** Ordinal of the column in the owning table. */
    private final int columnIndex;

    /** Index filter created from the Calcite expression. */
    private final IndexFilter filter;

    IndexComponentCandidate(
            RexNode expression,
            int columnIndex,
            IndexFilter filter
    ) {
        this.expression = expression;
        this.columnIndex = columnIndex;
        this.filter = filter;
    }

    RexNode getExpression() {
        return expression;
    }

    int getColumnIndex() {
        return columnIndex;
    }

    IndexFilter getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "IndexComponentCandidate {expression=" + expression + ", columnIndex=" + columnIndex + ", filter=" + filter + '}';
    }
}
