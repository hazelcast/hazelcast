/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import datamodel.AddToCart;
import datamodel.PageVisit;
import datamodel.Payment;

import java.util.Map.Entry;

public class ImplementAggregation {
    static void s1() {
        //tag::s1[]
        AggregateOperation1<Long, LongLongAccumulator, Double> aggrOp = AggregateOperation
                .withCreate(LongLongAccumulator::new)
                .<Long>andAccumulate((acc, n) -> {
                    acc.set1(acc.get1() + n);
                    acc.set2(acc.get2() + 1);
                })
                .andCombine((left, right) -> {
                    left.set1(left.get1() + right.get1());
                    left.set2(left.get2() + right.get2());
                })
                .andDeduct((left, right) -> {
                    left.set1(left.get1() - right.get1());
                    left.set2(left.get2() - right.get2());
                })
                .andExportFinish(acc -> (double) acc.get1() / acc.get2());
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        Pipeline p = Pipeline.create();
        BatchStage<PageVisit> pageVisit = p.drawFrom(Sources.list("pageVisit"));
        BatchStage<AddToCart> addToCart = p.drawFrom(Sources.list("addToCart"));
        BatchStage<Payment> payment = p.drawFrom(Sources.list("payment"));

        AggregateOperation3<PageVisit, AddToCart, Payment, LongAccumulator[], long[]>
            aggrOp = AggregateOperation
                .withCreate(() -> new LongAccumulator[] {
                        new LongAccumulator(),
                        new LongAccumulator(),
                        new LongAccumulator()
                })
                .<PageVisit>andAccumulate0((accs, pv) -> accs[0].add(pv.loadTime()))
                .<AddToCart>andAccumulate1((accs, atc) -> accs[1].add(atc.quantity()))
                .<Payment>andAccumulate2((accs, pm) -> accs[2].add(pm.amount()))
                .andCombine((accs1, accs2) -> {
                    accs1[0].add(accs2[0]);
                    accs1[1].add(accs2[1]);
                    accs1[2].add(accs2[2]);
                })
                .andExportFinish(accs -> new long[] {
                        accs[0].get(),
                        accs[1].get(),
                        accs[2].get()
                });

        BatchStage<Entry<Integer, long[]>> coGrouped =
                pageVisit.groupingKey(PageVisit::userId)
                         .aggregate3(
                                 addToCart.groupingKey(AddToCart::userId),
                                 payment.groupingKey(Payment::userId),
                                 aggrOp);
        //end::s2[]
    }
}
