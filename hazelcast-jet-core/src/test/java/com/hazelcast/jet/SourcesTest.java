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

import com.hazelcast.jet.function.DistributedFunction;
import org.junit.Test;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.projection.Projections.singleAttribute;
import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class SourcesTest extends PipelineTestSupport {

    @Test
    public void fromProcessor() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        pipeline.<Integer>drawFrom(Sources.fromProcessor("test",
                readMapP(srcName, truePredicate(),
                        (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue)))
                .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        pipeline.drawFrom(Sources.map(srcName))
                .drainTo(sink);
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjection() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        pipeline.drawFrom(Sources.map(srcName, truePredicate(), singleAttribute("value")))
                .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjectionFn() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        pipeline.drawFrom(Sources.map(
                        srcName, truePredicate(),
                        (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue))
                .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

//    @Test
//    public void remoteMap() {
//    }
//
//    @Test
//    public void remoteMapWithFilterAndProjection() {
//    }
//
//    @Test
//    public void remoteMapWithFilterAndProjectionFn() {
//    }
//
//    @Test
//    public void remoteMapJournal() {
//    }
//
//    @Test
//    public void remoteMapJournalWithPredicateAndProjectionFn() {
//    }
//
//    @Test
//    public void cache() {
//    }
//
//    @Test
//    public void cacheJournal() {
//    }
//
//    @Test
//    public void cacheJournalWithPredicateandProjectionFn() {
//    }
//
//    @Test
//    public void remoteCache() {
//    }
//
//    @Test
//    public void remoteCacheJournal() {
//    }
//
//    @Test
//    public void remoteCacheJournalWithPredicateAndProjectionFn() {
//    }
//
//    @Test
//    public void list() {
//    }
//
//    @Test
//    public void remoteList() {
//    }
//
//    @Test
//    public void socket() {
//    }
//
//    @Test
//    public void files() {
//    }
//
//    @Test
//    public void filesWithToString() {
//    }
//
//    @Test
//    public void fileChanges() {
//    }
//
//    @Test
//    public void fileChangesWithToString() {
//    }

}
