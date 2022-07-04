/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.processor.HashJoinCollectP.HashJoinArrayList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HashJoinPTest extends JetTestSupport {

    private static final BiFunction mapToOutputBiFn = Tuple2::tuple2;
    private static final TriFunction mapToOutputTriFn = Tuple3::tuple3;

    @Test
    public void test_oneToOneJoin_biJoin() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                singletonList(e -> e),
                emptyList(),
                mapToOutputBiFn,
                null,
                null
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(0, 1, 2),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(2, "b")
                        ))
                ), new int[]{ 10, 1 })
                .expectOutput(asList(
                        tuple2(0, null),
                        tuple2(1, "a"),
                        tuple2(2, "b")
                ));
    }

    @Test
    public void test_oneToNJoin_biJoin() {
        Function<Integer, Object> enrichingSideKeyFn = e -> e % 10;

        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                singletonList(enrichingSideKeyFn),
                emptyList(),
                mapToOutputBiFn,
                null,
                null
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(0, 1, 2),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(2, "b", "c")
                        ))

                ), new int[]{ 10, 1 })
                .expectOutput(asList(
                        tuple2(0, null),
                        tuple2(1, "a"),
                        tuple2(2, "b"),
                        tuple2(2, "c")
                ));
    }

    @Test
    public void test_oneToOneJoin_triJoin() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                asList(e -> e, e -> e),
                emptyList(),
                null,
                mapToOutputTriFn,
                null
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(1, 2, 3),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(3, "c")
                        )),
                        singletonList(toMap(
                                keyAndValues(1, "A"),
                                keyAndValues(2, "B")
                        ))
                ), new int[]{ 10, 1, 1 })
                .expectOutput(asList(
                        tuple3(1, "a", "A"),
                        tuple3(2, null, "B"),
                        tuple3(3, "c", null)
                ));
    }

    @Test
    public void test_oneToNJoin_triJoin() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                asList(e -> e, e -> e),
                emptyList(),
                null,
                mapToOutputTriFn,
                null
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(0, 1, 2, 3, 4),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(2, "b", "c"),
                                keyAndValues(4, "d", "e")
                        )),
                        singletonList(toMap(
                                keyAndValues(2, "A"),
                                keyAndValues(3, "B", "C"),
                                keyAndValues(4, "D", "E")
                        ))
                ), new int[]{ 10, 1, 1 })
                .expectOutput(asList(
                        tuple3(0, null, null),
                        tuple3(1, "a", null),
                        tuple3(2, "b", "A"),
                        tuple3(2, "c", "A"),
                        tuple3(3, null, "B"),
                        tuple3(3, null, "C"),
                        tuple3(4, "d", "D"),
                        tuple3(4, "d", "E"),
                        tuple3(4, "e", "D"),
                        tuple3(4, "e", "E")
                ));
    }

    @Test
    public void test_oneToOneJoin_withTags() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                asList(e -> e, e -> e),
                asList(tag0(), tag1()),
                mapToOutputBiFn,
                null,
                tupleToItemsByTag()
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(1, 2, 3),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(3, "c")
                        )),
                        singletonList(toMap(
                                keyAndValues(1, "A"),
                                keyAndValues(2, "B")
                        ))
                ), new int[]{ 10, 1, 1 })
                .expectOutput(asList(
                        tuple2(1, itemsByTag(tag0(), "a", tag1(), "A")),
                        tuple2(2, itemsByTag(tag0(), null, tag1(), "B")),
                        tuple2(3, itemsByTag(tag0(), "c", tag1(), null))
                ));
    }

    @Test
    public void test_oneToNJoin_withTags() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                asList(e -> e, e -> e),
                asList(tag0(), tag1()),
                mapToOutputBiFn,
                null,
                tupleToItemsByTag()
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(0, 1, 2, 3, 4),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(2, "b", "c"),
                                keyAndValues(4, "d", "e")
                        )),
                        singletonList(toMap(
                                keyAndValues(2, "A"),
                                keyAndValues(3, "B", "C"),
                                keyAndValues(4, "D", "E")
                        ))
                ), new int[]{ 10, 1, 1 })
                .expectOutput(asList(
                        tuple2(0, itemsByTag(tag0(), null, tag1(), null)),
                        tuple2(1, itemsByTag(tag0(), "a", tag1(), null)),
                        tuple2(2, itemsByTag(tag0(), "b", tag1(), "A")),
                        tuple2(2, itemsByTag(tag0(), "c", tag1(), "A")),
                        tuple2(3, itemsByTag(tag0(), null, tag1(), "B")),
                        tuple2(3, itemsByTag(tag0(), null, tag1(), "C")),
                        tuple2(4, itemsByTag(tag0(), "d", tag1(), "D")),
                        tuple2(4, itemsByTag(tag0(), "d", tag1(), "E")),
                        tuple2(4, itemsByTag(tag0(), "e", tag1(), "D")),
                        tuple2(4, itemsByTag(tag0(), "e", tag1(), "E"))
                ));
    }

    @Test
    public void test_biJoin_mapToNull() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                singletonList(e -> e),
                emptyList(),
                (l, r) -> r == null ? null : tuple2(l, r),
                null,
                null
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(0, 1),
                        singletonList(toMap(
                                keyAndValues(1, "a")
                        ))
                ), new int[]{ 10, 1 })
                .expectOutput(singletonList(
                        tuple2(1, "a")));
    }

    @Test
    public void test_triJoin_mapToNull() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                asList(e -> e, e -> e),
                emptyList(),
                null,
                (l, r1, r2) -> r1 == null || r2 == null ? null : tuple3(l, r1, r2),
                null
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(0, 1, 2, 3),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(2, "b")
                        )),
                        singletonList(toMap(
                                keyAndValues(1, "A"),
                                keyAndValues(3, "C")
                        ))
                ), new int[]{ 10, 1, 1 })
                .expectOutput(singletonList(
                        tuple3(1, "a", "A")));
    }

    @Test
    public void test_withTags_mapToNull() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                asList(e -> e, e -> e),
                asList(tag0(), tag1()),
                (item, itemsByTag) -> ((ItemsByTag) itemsByTag).get(tag0()) == null ? null : tuple2(item, itemsByTag),
                null,
                tupleToItemsByTag()
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(1, 2, 3),
                        singletonList(toMap(
                                keyAndValues(1, "a"),
                                keyAndValues(3, "c")
                        )),
                        singletonList(toMap(
                                keyAndValues(1, "A"),
                                keyAndValues(2, "B")
                        ))
                ), new int[]{ 10, 1, 1 })
                .expectOutput(asList(
                        tuple2(1, itemsByTag(tag0(), "a", tag1(), "A")),
                        tuple2(3, itemsByTag(tag0(), "c", tag1(), null))
                ));

    }

    @Test
    public void when_arrayListInItems_then_treatedAsAnItem() {
        SupplierEx<Processor> supplier = () -> new HashJoinP<>(
                singletonList(e -> e),
                emptyList(),
                mapToOutputBiFn,
                null,
                null
        );

        List<String> listItem = new ArrayList<>();
        listItem.add("a");
        listItem.add("b");
        verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        singletonList(0),
                        singletonList(toMap(
                                keyAndValues(0, listItem)
                        ))
                ), new int[]{ 10, 1 })
                .expectOutput(singletonList(
                        tuple2(0, listItem)
                ));
    }

    @SafeVarargs
    private static <K, V> Tuple2<K, Object> keyAndValues(K key, V... args) {
        assert args.length > 0;
        if (args.length > 1) {
            HashJoinArrayList list = new HashJoinArrayList();
            list.addAll(Arrays.asList(args));
            return tuple2(key, list);
        }
        return tuple2(key, args[0]);
    }

    @SafeVarargs
    private static <K, V> Map<K, Object> toMap(Tuple2<K, Object>... entries) {
        return Stream.of(entries).collect(Collectors.toMap(Tuple2::f0, Tuple2::f1));
    }

    private static BiFunctionEx<List<Tag>, Object[], ItemsByTag> tupleToItemsByTag() {
        return (tagList, tuple) -> {
            ItemsByTag res = new ItemsByTag();
            for (int i = 0; i < tagList.size(); i++) {
                res.put(tagList.get(i), tuple[i]);
            }
            return res;
        };
    }
}
