package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.query.QueryHandle;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.internal.query.expression.ConstantExpression;
import com.hazelcast.internal.query.expression.EqualsPredicate;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.expression.ExtractorExpression;
import com.hazelcast.internal.query.expression.OrPredicate;
import com.hazelcast.internal.query.expression.Predicate;
import com.hazelcast.internal.query.physical.MapScanPhysicalNode;
import com.hazelcast.internal.query.physical.PhysicalPlan;
import com.hazelcast.internal.query.physical.ReceivePhysicalNode;
import com.hazelcast.internal.query.physical.RootPhysicalNode;
import com.hazelcast.internal.query.physical.SendPhysicalNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NewQueryTest extends HazelcastTestSupport {
    @Test
    public void testScanSimple() throws Exception {
        // Start several members.
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config cfg = new Config();

        HazelcastInstance member1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance member2 = nodeFactory.newHazelcastInstance(cfg);

        // Insert data.
        for (int i = 0; i < 100; i++)
            member1.getMap("queryMap").put(i, new Person(i));

        // Prepare a plan.

        // Root fragment: delivers data to the user.
        ReceivePhysicalNode receiveNode = new ReceivePhysicalNode(
            1, // Edge 1
            1  // Parallelism 1
        );

        RootPhysicalNode rootNode = new RootPhysicalNode(
            receiveNode // Consume data from the receiver.
        );

        // Scan the map.
        List<Expression> projection = asList(new ExtractorExpression("id"), new ExtractorExpression("name"));
        Predicate filter =
            new OrPredicate(
                new EqualsPredicate(new ExtractorExpression("id"), new ConstantExpression<>(1)),
                new EqualsPredicate(new ExtractorExpression("id"), new ConstantExpression<>(2))
            );

        MapScanPhysicalNode scanNode = new MapScanPhysicalNode(
            "queryMap", // Scan
            projection, // Project
            filter,     // Filter
            1           // Parallelism
        );

        // Send scan results to the edge 1.
        SendPhysicalNode sendNode = new SendPhysicalNode(
            1,                           // Edge
            scanNode,                    // Underlying scan
            new ConstantExpression<>(1), // Partitioning info: REWORK!
            false                        // Partitioning info: REWORK!
        );

        // Physical plan with two fragments
        PhysicalPlan plan = new PhysicalPlan();

        plan.addNode(rootNode);
        plan.addNode(sendNode);

        QueryService service = ((HazelcastInstanceProxy)member1).getOriginal().getQueryService();

        QueryHandle handle = service.execute(plan, null);

        // TODO: Because we cannot wait for completion yet.
        Thread.sleep(Long.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> asList(T... elems) {
        if (elems == null || elems.length == 0)
            return Collections.emptyList();

        ArrayList<T> res = new ArrayList<>(elems.length);

        Collections.addAll(res, elems);

        return res;
    }

    private static class Person implements Serializable {
        private final int id;
        private final String name;

        public Person(int id) {
            this.id = id;
            this.name = "person-" + id;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
