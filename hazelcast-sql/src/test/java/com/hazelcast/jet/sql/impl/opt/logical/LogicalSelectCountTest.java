package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.nojobshortcuts.MapSizeRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalSelectCountTest extends OptimizerTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void test_positiveCases() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertPlan(
                optimizeNoJobShortcuts("SELECT count(*) FROM m", table),
                plan(planRow(0, MapSizeRel.class)));
        assertPlan(
                optimizeNoJobShortcuts("SELECT count(*) FROM (SELECT * FROM m)", table),
                plan(planRow(0, MapSizeRel.class)));
        assertPlan(
                optimizeNoJobShortcuts("SELECT count(*) FROM (SELECT __key FROM m)", table),
                plan(planRow(0, MapSizeRel.class)));
        assertPlan(
                optimizeNoJobShortcuts("SELECT count(*) FROM (SELECT 1 FROM m)", table),
                plan(planRow(0, MapSizeRel.class)));
        // these should be supported, but currently are not, because `__key` and `this` aren't not-null
//        assertPlan(
//                optimizeNoJobShortcuts("SELECT count(__key) FROM m", table),
//                plan(planRow(0, MapSizeRel.class)));
//        assertPlan(
//                optimizeNoJobShortcuts("SELECT count(this) FROM m", table),
//                plan(planRow(0, MapSizeRel.class)));
    }

    @Test
    public void test_negativeCases() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertFailure("SELECT count(__key) FROM m", table);
        assertFailure("SELECT count(this) FROM m", table);
        assertFailure("SELECT count(*) FROM m GROUP BY this", table);
        assertFailure("SELECT count(*) FROM m WHERE this='a'", table);
        assertFailure("SELECT count(*), count(*) FROM m", table);
        assertFailure("SELECT count(*) + 1 FROM m", table);
    }

    private void assertFailure(String sql, HazelcastTable table) {
        assertThatThrownBy(() -> optimizeNoJobShortcuts(sql, table))
                .isInstanceOf(CannotPlanException.class)
                .hasMessageStartingWith("There are not enough rules to produce a node with desired properties");
    }
}
