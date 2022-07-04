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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.VertexWithInputConfig;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.extract.QueryPath;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PATH;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PATH;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;

@RunWith(JUnitParamsRunner.class)
public class JoinerTest {

    @Mock
    private DAG dag;

    @Mock
    private Vertex vertex;

    @Mock
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @SuppressWarnings("unused")
    private Object[] joinTypes() {
        return new Object[]{INNER, LEFT};
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPrimitiveKey(JoinRelType joinType) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{KEY_PATH});
        given(dag.newUniqueVertex(contains("Lookup"), isA(JoinByPrimitiveKeyProcessorSupplier.class))).willReturn(vertex);

        // when
        VertexWithInputConfig vertexWithConfig = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(joinType, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(vertexWithConfig.vertex()).isEqualTo(vertex);
        assertThat(vertexWithConfig.configureEdgeFn()).isNotNull();
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPredicate(JoinRelType joinType) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{QueryPath.create("path")});
        given(dag.newUniqueVertex(contains("Predicate"), isA(ProcessorMetaSupplier.class))).willReturn(vertex);

        // when
        VertexWithInputConfig vertexWithConfig = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(joinType, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(vertexWithConfig.vertex()).isEqualTo(vertex);
        assertThat(vertexWithConfig.configureEdgeFn()).isNotNull();
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByScan(JoinRelType joinType) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{VALUE_PATH});
        given(dag.newUniqueVertex(contains("Scan"), isA(JoinScanProcessorSupplier.class))).willReturn(vertex);

        // when
        VertexWithInputConfig vertexWithConfig = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(joinType, new int[0], new int[0]),
                rightRowProjectorSupplier
        );

        // then
        assertThat(vertexWithConfig.vertex()).isEqualTo(vertex);
        assertThat(vertexWithConfig.configureEdgeFn()).isNull();
    }

    private static JetJoinInfo joinInfo(JoinRelType joinType, int[] leftEquiJoinIndices, int[] rightEquiJoinIndices) {
        return new JetJoinInfo(joinType, leftEquiJoinIndices, rightEquiJoinIndices, null, null);
    }
}
