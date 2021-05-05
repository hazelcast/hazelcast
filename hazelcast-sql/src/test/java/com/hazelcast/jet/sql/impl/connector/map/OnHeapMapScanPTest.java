package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.Util.entry;

@Category({QuickTest.class, ParallelJVMTest.class})
public class OnHeapMapScanPTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void setUp() {
        initialize(1, null);
    }

//    @Test
//    public void test_whenNoPredicateAndNoProjection() {
//        IMap<Integer, String> map = instance().getMap(randomMapName());
//        List<Map.Entry<Integer, String>> expected = new ArrayList<>();
//        for (int i = 0; i < 1000; i++) {
//            map.put(i, "value-" + i);
//            expected.add(entry(i, "value-" + i));
//        }
//
//        TestSupport
//                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName())))
//                .jetInstance(instance())
//                .disableSnapshots()
//                .disableProgressAssertion()
//                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
//                .expectOutput(expected);
//    }

}
