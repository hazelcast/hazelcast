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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceP;
import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;

public final class SqlProcessors {

    private SqlProcessors() {
    }

    public static ProcessorSupplier rowProjector(
            String[] paths,
            QueryDataType[] types,
            SupplierEx<QueryTarget> targetSupplier,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        ServiceFactory<?, RowProjector> service =
                nonSharedService(ctx -> new RowProjector(paths, types, targetSupplier.get(), predicate, projection,
                        ExpressionEvalContext.from(ctx)));
        return mapUsingServiceP(service, RowProjector::project);
    }
}
