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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;

class KvRowProjector implements Row {

    private final QueryTarget keyTarget;
    private final QueryTarget valueTarget;
    private final QueryExtractor[] extractors;

    private final Expression<Boolean> predicate;
    private final List<Expression<?>> projection;

    @SuppressWarnings("unchecked")
    KvRowProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTarget keyTarget,
            QueryTarget valueTarget,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        this.keyTarget = keyTarget;
        this.valueTarget = valueTarget;
        this.extractors = createExtractors(paths, types, keyTarget, valueTarget);

        this.predicate = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(true, QueryDataType.BOOLEAN);
        this.projection = projection;
    }

    private static QueryExtractor[] createExtractors(
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTarget keyTarget,
            QueryTarget valueTarget
    ) {
        QueryExtractor[] extractors = new QueryExtractor[paths.length];
        for (int i = 0; i < paths.length; i++) {
            QueryPath path = paths[i];
            QueryDataType type = types[i];

            extractors[i] = path.isKey()
                    ? keyTarget.createExtractor(path.getPath(), type)
                    : valueTarget.createExtractor(path.getPath(), type);
        }
        return extractors;
    }

    public Object[] project(Entry<Object, Object> entry) {
        keyTarget.setTarget(entry.getKey());
        valueTarget.setTarget(entry.getValue());

        if (!Boolean.TRUE.equals(evaluate(predicate, this))) {
            return null;
        }

        Object[] row = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            row[i] = evaluate(projection.get(i), this);
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int index) {
        return (T) extractors[index].get();
    }

    @Override
    public int getColumnCount() {
        return extractors.length;
    }
}
