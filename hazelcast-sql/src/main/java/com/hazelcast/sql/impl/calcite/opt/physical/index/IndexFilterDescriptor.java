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
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class IndexFilterDescriptor {
    /** Filter that will be used during query execution. */
    private final IndexFilter filter;

    /** Calcite expressions that formed the filter */
    private final List<RexNode> expressions;

    public IndexFilterDescriptor(IndexFilter filter, RexNode expression) {
        this.filter = filter;
        this.expressions = Collections.singletonList(expression);
    }

    public IndexFilterDescriptor(IndexFilter filter, List<RexNode> expressions) {
        this.filter = filter;
        this.expressions = expressions;
    }

    public IndexFilter getFilter() {
        return filter;
    }

    public List<RexNode> getExpressions() {
        return expressions;
    }
}
