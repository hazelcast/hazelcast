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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.RowProjectorProcessorSupplier;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
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
                        SimpleExpressionEvalContext.from(ctx)));
        return mapUsingServiceP(service, RowProjector::project);
    }

    public static ProcessorSupplier rowProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        return new RowProjectorProcessorSupplier(
                KvRowProjector.supplier(paths, types, keyDescriptor, valueDescriptor, predicate, projection));
    }
}
