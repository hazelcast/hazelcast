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

import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Filter of a single component of the index.
 * <p>
 * A filter is formed from one or more subexpressions of the original expression. We track these original expressions to
 * exclude them from the remainder filter that will be applied to the index scan operator. For example, the expression
 * {@code WHERE a>? AND a<?}, the {@link IndexRangeFilter} will be formed, with two expressions {@code a>?} and {@code a<?}
 * tracked.
 * <p>
 * The component filter always concerned only with a single component of the filter. In other words, only a single column
 * participates in the filter. On the later planning stages several component filters of the same index are merged into a
 * single composite filter that will be passed to the operator.
 */
public class IndexComponentFilter {
    /** Filter that will be executed by the operator. */
    private final IndexFilter filter;

    /** Calcite expressions that formed the filter. These expressions will be excluded from the remainder filter. */
    private final List<RexNode> expressions;

    /** Expected converter type of the target index. If converter type doesn't match, an execution exception will be thrown. */
    private final QueryDataType converterType;

    public IndexComponentFilter(IndexFilter filter, List<RexNode> expressions, QueryDataType converterType) {
        this.filter = filter;
        this.expressions = expressions;
        this.converterType = converterType;
    }

    public IndexFilter getFilter() {
        return filter;
    }

    public List<RexNode> getExpressions() {
        return expressions;
    }

    public QueryDataType getConverterType() {
        return converterType;
    }

    @Override
    public String toString() {
        return "IndexComponentFilter {filter=" + filter + ", expressions=" + expressions + ", converter=" + converterType + '}';
    }
}
