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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.WatermarkKeysAssigner;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class CreateDagVisitorPrunabilityLevelsTest extends OptimizerTestSupport {
    private static final String MAP_NAME = "m";

    private HazelcastTable table;
    List<TableField> mapTableFields;
    CreateTopLevelDagVisitor visitor;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(5, null);
    }

    @Before
    public void before() throws Exception {
        this.mapTableFields = asList(
                mapField(KEY, OBJECT, QueryPath.KEY_PATH),
                mapField("comp0", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp0")),
                mapField("comp1", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp1")),
                mapField(VALUE, BIGINT, QueryPath.VALUE_PATH));

        this.table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                emptyList(),
                10,
                singletonList("comp1"));
    }

    @Test
    public void test_requiresCoordinatorOnly() {
        PhysicalRel rel = optimizePhysical("SELECT * FROM m WHERE comp1 = 10", asList(BIGINT, BIGINT), table)
                .getPhysical();
        assertPlan(rel, plan(planRow(0, FullScanPhysicalRel.class)));

        RootRel rootRel = new RootRel(rel);

        this.visitor = new CreateTopLevelDagVisitor(
                getNodeEngineImpl(instance()),
                QueryParameterMetadata.EMPTY,
                new WatermarkKeysAssigner(rel),
                singleton(table.getTarget().getObjectKey()));

        rootRel.accept(visitor);
    }

    @Test
    public void test_requiresAllPartitionsOnly() {
        PhysicalRel rel = optimizePhysical("SELECT comp1, MAX(comp1) FROM " + MAP_NAME + " GROUP BY comp1",
                asList(BIGINT, BIGINT), table)
                .getPhysical();
        assertPlan(rel, plan(
                planRow(0, AggregateCombineByKeyPhysicalRel.class),
                planRow(1, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class)
        ));

        // This test doesn't contain RootRel, what forces to distribute all items to coordinator.
        this.visitor = new CreateTopLevelDagVisitor(
                getNodeEngineImpl(instance()),
                QueryParameterMetadata.EMPTY,
                new WatermarkKeysAssigner(rel),
                singleton(table.getTarget().getObjectKey()));

        rel.accept(visitor);
    }

    @Test
    public void test_requiresAllPartitionsAndCoordinator() {
        PhysicalRel rel = optimizePhysical("SELECT comp1, MAX(comp1) FROM " + MAP_NAME + " GROUP BY comp1",
                asList(BIGINT, BIGINT), table)
                .getPhysical();
        assertPlan(rel, plan(
                planRow(0, AggregateCombineByKeyPhysicalRel.class),
                planRow(1, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class)
        ));

        RootRel rootRel = new RootRel(rel);

        this.visitor = new CreateTopLevelDagVisitor(
                getNodeEngineImpl(instance()),
                QueryParameterMetadata.EMPTY,
                new WatermarkKeysAssigner(rel),
                singleton(table.getTarget().getObjectKey()));

        rootRel.accept(visitor);
    }
}
