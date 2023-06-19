package com.hazelcast.jet.sql.impl.opt.prunability;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.extract.QueryPath;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FullScanPrunabilityTest extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test() {
        HazelcastTable table = partitionedTable(
                "m",
                asList(
                        mapField(KEY, INT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                10);
        PhysicalRel root = optimizePhysical("SELECT __key FROM m WHERE this = '10'", asList(INT, VARCHAR), table).getPhysical();

        assertPlan(root, plan(planRow(0, FullScanPhysicalRel.class)));

        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        List<Tuple2<String, Map<String, RexNode>>> prunability = query.extractPrunability(root);
        assertEquals(1, prunability.size());
        assertNotNull(prunability.get(0));
        assertEquals("m", prunability.get(0).f0());

        Map<String, RexNode> completenessMap = prunability.get(0).f1();
        assertEquals(1, completenessMap.size());

        RexInputRef expectedLeftInputRef = new RexInputRef(1,
                HazelcastTypeUtils.createType(
                        HazelcastTypeFactory.INSTANCE,
                        SqlTypeName.VARCHAR,
                        false));

        assertTrue(completenessMap.containsKey(expectedLeftInputRef.getName()));
        assertInstanceOf(RexCall.class, completenessMap.get(expectedLeftInputRef.getName()));
    }
}
