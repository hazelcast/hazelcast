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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;

/**
 * Filter executor.
 */
public class FilterExec extends AbstractFilterExec {

    private final Expression<Boolean> filter;

    public FilterExec(int id, Exec upstream, Expression<Boolean> filter) {
        super(id, upstream);

        this.filter = filter;
    }

    @Override
    protected boolean eval(Row row) {
        Boolean res = filter.eval(row, ctx);

        return res != null && res;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }
}
