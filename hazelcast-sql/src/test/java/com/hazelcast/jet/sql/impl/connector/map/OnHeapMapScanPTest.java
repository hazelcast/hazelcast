package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.ConstantPredicateExpression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static java.util.Collections.emptyList;

@Category({QuickTest.class, ParallelJVMTest.class})
public class OnHeapMapScanPTest extends SimpleTestInClusterSupport {

    protected static volatile AtomicInteger id;

    @BeforeClass
    public static void setUp() {
        id = new AtomicInteger(0);
        initialize(1, null);
    }

    @Test
    public void test_whenNoPredicateAndNoProjection() {

        IMap<Integer, String> map = instance().getMap(randomMapName());
        List<Map.Entry<Integer, String>> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            map.put(i, "value-" + i);
            expected.add(entry(i, "value-" + i));
        }

        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        final NodeEngine nodeEngine = ((MapService) mapProxy.getService()).getMapServiceContext().getNodeEngine();

        MapScanPlanNode scanNode = new MapScanPlanNode(
                id.getAndIncrement(),
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Collections.singletonList(valuePath("this")),
                Collections.singletonList(QueryDataType.VARCHAR),
                Collections.emptyList(),
                new ConstantPredicateExpression(true)
        );

        TestSupport
                .verifyProcessor(getProcessorSupplierEx(map.getName(), nodeEngine, scanNode))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(expected);
    }

    private SupplierEx<Processor> getProcessorSupplierEx(
            String mapName,
            NodeEngine nodeEngine,
            MapScanPlanNode node
    ) {
        return () -> new OnHeapMapScanP(mapName, nodeEngine, node);
    }

}
