/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.function.DistributedFunction;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.util.stream.Collectors.toList;

public class PipelineTestSupport extends TestInClusterSupport {

    static final char ITEM_COUNT = 10;
    final String srcName = randomName();
    final String sinkName = randomName();

    Pipeline pipeline;
    ComputeStage<Integer> srcStage;
    Sink<Object> sink;

    IMap<String, Integer> srcMap;
    IList<Object> sinkList;

    @Before
    public void beforePipelineTestSupport() {
        pipeline = Pipeline.create();
        srcMap = jet().getMap(srcName);
        sink = Sinks.list(sinkName);
        sinkList = jet().getList(sinkName);
    }

    void putToSrcMap(List<Integer> data) {
        putToMap(srcMap, data);
    }

    static void putToMap(Map<String, Integer> dest, List<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    JetInstance jet() {
        return testMode.getJet();
    }

    void execute() {
        jet().newJob(pipeline).join();
    }

    Map<Object, Integer> sinkToBag() {
        return toBag(this.sinkList);
    }

    static Source<Integer> mapValuesSource(String srcName) {
        return Sources.map(srcName, truePredicate(),
                (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue);
    }

    static <T> Map<T, Integer> toBag(Collection<T> coll) {
        Map<T, Integer> bag = new HashMap<>();
        for (T t : coll) {
            bag.merge(t, 1, (count, x) -> count + 1);
        }
        return bag;
    }

    static List<Integer> sequence(int itemCount) {
        return IntStream.range(0, itemCount).boxed().collect(toList());
    }
}
