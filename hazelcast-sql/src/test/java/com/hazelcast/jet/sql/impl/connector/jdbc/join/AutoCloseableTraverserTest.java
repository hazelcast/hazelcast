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

package com.hazelcast.jet.sql.impl.connector.jdbc.join;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.processor.TransformBatchedP;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.junit.Test;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class AutoCloseableTraverserTest extends JetTestSupport {

    @Test
    public void test_all_streams_are_closed() {
        // Create stream 1
        FullScanResultSetIterator<JetSqlRow> iterator = new FullScanResultSetIterator<>(
                null,
                null,
                null,
                null
        );

        Spliterator<JetSqlRow> spliterator = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.IMMUTABLE | Spliterator.ORDERED);

        Stream<JetSqlRow> iteratorStream = StreamSupport.stream(spliterator, false).onClose(iterator::close);

        // Create stream 2
        final AtomicBoolean wasClosed = new AtomicBoolean(false);
        JetSqlRow jetSqlRow = new JetSqlRow();
        Stream<JetSqlRow> stream2 = Stream.ofNullable(jetSqlRow).onClose(() -> wasClosed.set(true));

        // Concat streams
        SupplierEx<Processor> processorSupplierEx = getProcessorSupplierEx(iteratorStream, stream2);

        // When processor is closed, we expect the stream to be closed too
        TestSupport
                .verifyProcessor(processorSupplierEx)
                .input(asList(1, 2, 3, 4, 5))
                .expectOutput(asList(1, 2, 3, 4, 5));

        assertThat(iterator.isIteratorClosed()).isTrue();
        assertThat(wasClosed.get()).isTrue();
    }

    @Test
    public void test_iterator_is_closed() {
        JoinPredicateScanResultSetIterator<Integer> iterator = new JoinPredicateScanResultSetIterator<>(null, null, null, null);

        Function<? super Iterable<Integer>, Traverser<Integer>> mapper =
                (Iterable<Integer> items) ->
                        new AutoCloseableTraverser<>(iterator, Traversers.traverseIterable(items));

        TransformBatchedP<Integer, Integer> processor = new TransformBatchedP<>(mapper);
        SupplierEx<Processor> processorSupplierEx = () -> processor;

        // When processor is closed, we expect the iterator to be closed too
        TestSupport
                .verifyProcessor(processorSupplierEx)
                .input(asList(1, 2, 3, 4, 5))
                .expectOutput(asList(1, 2, 3, 4, 5));

        assertThat(iterator.isIteratorClosed()).isTrue();
    }

    private static SupplierEx<Processor> getProcessorSupplierEx(Stream<JetSqlRow> iteratorStream,
                                                                Stream<JetSqlRow> stream2) {
        Stream<JetSqlRow> stream = Stream.concat(iteratorStream, stream2);

        // Use stream as AutoCloseable
        Function<? super Iterable<Integer>, Traverser<Integer>> mapper =
                (Iterable<Integer> items) ->
                        new AutoCloseableTraverser<>(stream, Traversers.traverseIterable(items));

        var processor = new TransformBatchedP<>(mapper);
        return () -> processor;
    }
}
