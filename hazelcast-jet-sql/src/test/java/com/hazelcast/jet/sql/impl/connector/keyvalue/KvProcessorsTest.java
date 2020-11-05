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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class KvProcessorsTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    @SuppressWarnings("unchecked")
    public void test_rowProjectorSerialization() {
        ProcessorSupplier original = KvProcessors.rowProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.INT},
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                (Expression<Boolean>) ConstantExpression.create(Boolean.FALSE, QueryDataType.BOOLEAN),
                asList(ConstantExpression.create(1, QueryDataType.INT), ConstantExpression.create("2", QueryDataType.INT))
        );

        ProcessorSupplier serialized = SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }

    @Test
    public void test_entryRowProjectorSerialization() {
        ProcessorSupplier original = KvProcessors.entryProjector(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, QueryDataType.VARCHAR},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE
        );

        ProcessorSupplier serialized = SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
