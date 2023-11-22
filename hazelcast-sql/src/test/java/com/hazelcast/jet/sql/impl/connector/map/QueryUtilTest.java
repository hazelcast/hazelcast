/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.security.auth.Subject;

import static com.hazelcast.jet.sql.impl.connector.map.QueryUtil.toPredicate;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryUtilTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void when_leftValueIsNull_then_returnsNull() {
        Predicate<Object, Object> predicate =
                toPredicate(new JetSqlRow(TEST_SS, new Object[1]), new int[]{0}, new int[]{0}, new QueryPath[]{QueryPath.KEY_PATH});

        assertThat(predicate).isNull();
    }

    @Test
    public void when_serializedObject_then_deserializedCorrect() {
        AbstractSerializationService service = (AbstractSerializationService) TestUtil.getNode(instance()).getSerializationService();

        var evalContextMock = mock(ExpressionEvalContext.class);
        when(evalContextMock.getSerializationService()).thenReturn(mock());
        var subject = new Subject(true, emptySet(), emptySet(), emptySet());
        when(evalContextMock.subject()).thenReturn(subject);

        var supplier = KvRowProjector.supplier(
                new QueryPath[]{},
                new QueryDataType[]{},
                null,
                null,
                null,
                null
        );
        DataSerializable projection = (DataSerializable) QueryUtil.toProjection(supplier, evalContextMock);

        var data = service.toData(projection);
        var actual = service.toObject(data);

        assertThat(actual)
                .usingRecursiveComparison()
                .comparingOnlyFields("rowProjectorSupplier", "arguments", "subject")
                .ignoringFields("subject.pubCredentials", "subject.privCredentials")
                .isEqualTo(projection);
    }
}
