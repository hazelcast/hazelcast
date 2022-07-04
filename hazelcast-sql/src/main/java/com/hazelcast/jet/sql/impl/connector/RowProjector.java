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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;

public class RowProjector implements Row {

    private final QueryTarget target;
    private final QueryExtractor[] extractors;

    private final Expression<Boolean> predicate;
    private final List<Expression<?>> projection;
    private final ExpressionEvalContext evalContext;

    @SuppressWarnings("unchecked")
    public RowProjector(
            String[] paths,
            QueryDataType[] types,
            QueryTarget target,
            Expression<Boolean> predicate,
            List<Expression<?>> projection,
            ExpressionEvalContext evalContext
    ) {
        checkTrue(paths.length == types.length, "paths.length != types.length");
        this.target = target;
        this.extractors = createExtractors(target, paths, types);

        this.predicate = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(true, QueryDataType.BOOLEAN);
        this.projection = projection;
        this.evalContext = evalContext;
    }

    private static QueryExtractor[] createExtractors(
            QueryTarget target,
            String[] paths,
            QueryDataType[] types
    ) {
        QueryExtractor[] extractors = new QueryExtractor[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            QueryDataType type = types[i];

            extractors[i] = target.createExtractor(path, type);
        }
        return extractors;
    }

    public JetSqlRow project(Object object) {
        target.setTarget(object, null);

        if (!Boolean.TRUE.equals(evaluate(predicate, this, evalContext))) {
            return null;
        }

        Object[] row = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            row[i] = evaluate(projection.get(i), this, evalContext);
        }
        return new JetSqlRow(evalContext.getSerializationService(), row);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int index) {
        return (T) extractors[index].get();
    }

    @Override
    public int getColumnCount() {
        return extractors.length;
    }
}
