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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.test.impl.ItemsDistributedFillBufferFn;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Contains factory methods for various mock sources which can be used for
 * pipeline testing and development.
 *
 * @since Jet 3.2
 */
@EvolvingApi
public final class TestSources {

    private TestSources() {
    }

    /**
     * Returns a batch source which iterates through the supplied iterable and
     * then terminates. The source is non-distributed.
     *
     * @since Jet 3.2
     */
    @Nonnull
    public static <T> BatchSource<T> items(@Nonnull Iterable<? extends T> items) {
        Objects.requireNonNull(items, "items");
        return SourceBuilder.batch("items", ctx -> null)
            .<T>fillBufferFn((ignored, buf) -> {
                items.forEach(buf::add);
                buf.close();
            }).build();
    }

    /**
     * Returns a batch source which iterates through the supplied items and
     * then terminates. The source is non-distributed.
     *
     * @since Jet 3.2
     */
    @Nonnull
    public static <T> BatchSource<T> items(@Nonnull T... items) {
        Objects.requireNonNull(items, "items");
        return items(Arrays.asList(items));
    }

    /**
     * Returns a batch source which iterates through the supplied iterable and
     * then terminates. The source is distributed - a slice of the items is
     * emitted on each member with local parallelism of 1.
     *
     * @since Jet 4.4
     */
    @Nonnull
    public static <T> BatchSource<T> itemsDistributed(@Nonnull Iterable<? extends T> items) {
        Objects.requireNonNull(items, "items");
        return SourceBuilder.batch("items", ctx -> ctx)
                .<T>fillBufferFn(new ItemsDistributedFillBufferFn<>(items))
                .distributed(1)
                .build();
    }

    /**
     * Returns a batch source which iterates through the supplied items and
     * then terminates. The source is distributed - a slice of the items is
     * emitted on each member with local parallelism of 1.
     *
     * @since Jet 4.4
     */
    @Nonnull
    public static <T> BatchSource<T> itemsDistributed(@Nonnull T... items) {
        Objects.requireNonNull(items, "items");
        return itemsDistributed(Arrays.asList(items));
    }

    /**
     * Returns a streaming source that generates events of type {@link
     * SimpleEvent} at the specified rate.
     * <p>
     * The source supports {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) native timestamps}. The
     * timestamp is the current system time at the moment they are generated.
     * The source is not distributed and all the items are generated on the
     * same node. This source is not fault-tolerant. The sequence will be
     * reset once a job is restarted.
     * <p>
     * <strong>Note:</strong>
     * There is no absolute guarantee that the actual rate of emitted items
     * will match the supplied value. It is ensured that no emitted event's
     * timestamp will be in the future.
     *
     * @param itemsPerSecond how many items should be emitted each second
     *
     * @since Jet 3.2
     */
    @EvolvingApi
    @Nonnull
    public static StreamSource<SimpleEvent> itemStream(int itemsPerSecond) {
        return itemStream(itemsPerSecond, SimpleEvent::new);
    }

    /**
     * Returns a streaming source that generates events created by the {@code
     * generatorFn} at the specified rate.
     * <p>
     * The source supports {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) native timestamps}. The
     * timestamp is the current system time at the moment they are generated.
     * The source is not distributed and all the items are generated on the
     * same node. This source is not fault-tolerant. The sequence will be
     * reset once a job is restarted.
     * <p>
     * <strong>Note:</strong>
     * There is no absolute guarantee that the actual rate of emitted items
     * will match the supplied value. It is ensured that no emitted event's
     * timestamp will be in the future.
     *
     * @param itemsPerSecond how many items should be emitted each second
     * @param generatorFn a function which takes the timestamp and the sequence of the generated
     *                    item and maps it to the desired type
     *
     * @since Jet 3.2
     */
    @EvolvingApi
    @Nonnull
    public static <T> StreamSource<T> itemStream(
        int itemsPerSecond,
        @Nonnull GeneratorFunction<? extends T> generatorFn
    ) {
        Objects.requireNonNull(generatorFn, "generatorFn");
        checkSerializable(generatorFn, "generatorFn");

        return SourceBuilder.timestampedStream("itemStream", ctx -> new ItemStreamSource<T>(itemsPerSecond, generatorFn))
            .<T>fillBufferFn(ItemStreamSource::fillBuffer)
            .build();
    }

    /**
     * Returns a {@link StreamSource} that emits an ever-increasing sequence of
     * {@code Long} numbers with native timestamps that are exactly the same
     * amount of time apart, as specified by the supplied {@code
     * eventsPerSecond} parameter. The source is distributed and suitable for
     * high-throughput performance testing. It emits the events at the maximum
     * possible speed, constrained by the invariant that it will never emit an
     * event whose timestamp is in the future.
     * <p>
     * The emission of events is distributed across the parallel processors in
     * a round-robin fashion: processor 0 emits the first event, processor 1
     * the second one, and so on. There is no coordination that would prevent
     * processor 1 from emitting its event before processor 0, though, so this
     * only applies to the event timestamps.
     * <p>
     * Use the {@code initialDelayMillis} parameter to give enough time to the
     * Jet cluster to initialize the job on the whole cluster before the time
     * of the first event arrives, so that there is no initial flood of events
     * from the past. The point of reference is the moment at which the
     * coordinator node creates the job's execution plan, before sending it out
     * to the rest of the cluster.
     * <p>
     * This source is not fault-tolerant. The sequence will be reset once a job
     * is restarted.
     * <p>
     * <strong>Note:</strong>
     * A clock skew between any two cluster members may result in an artificial
     * increase of latency.
     *
     * @param eventsPerSecond the desired event rate
     * @param initialDelayMillis initial delay in milliseconds before emitting values
     *
     * @since Jet 4.3
     */
    @Nonnull
    public static StreamSource<Long> longStream(long eventsPerSecond, long initialDelayMillis) {
        return Sources.streamFromProcessorWithWatermarks("longStream",
                true,
                eventTimePolicy -> {
                    long startTime = System.currentTimeMillis() + initialDelayMillis;
                    return ProcessorMetaSupplier.of(() ->
                            new LongStreamSourceP(startTime, eventsPerSecond, eventTimePolicy));
                }
        );
    }

    private static final class ItemStreamSource<T> {
        private static final int MAX_BATCH_SIZE = 1024;

        private final GeneratorFunction<? extends T> generator;
        private final long periodNanos;

        private long emitSchedule;
        private long sequence;

        private ItemStreamSource(int itemsPerSecond, GeneratorFunction<? extends T> generator) {
            this.periodNanos = Math.max(TimeUnit.SECONDS.toNanos(1) / itemsPerSecond, 1);
            this.generator = generator;
        }

        void fillBuffer(TimestampedSourceBuffer<T> buf) throws Exception {
            long nowNs = System.nanoTime();
            if (emitSchedule == 0) {
                emitSchedule = nowNs;
            }
            // round ts down to nearest period
            long tsNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
            long ts = TimeUnit.NANOSECONDS.toMillis(tsNanos - (tsNanos % periodNanos));
            for (int i = 0; i < MAX_BATCH_SIZE && nowNs >= emitSchedule; i++) {
                T item = generator.generate(ts, sequence++);
                buf.add(item, ts);
                emitSchedule += periodNanos;
            }
        }
    }
}
