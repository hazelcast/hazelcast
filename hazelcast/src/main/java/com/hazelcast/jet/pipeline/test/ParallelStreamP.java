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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;


/**
 * Implements a mock stream source with parallelism. It emits the items
 * from the generator functions you supply, preserving the order of each
 * generator.
 *
 * @param <T> type of emitted items
 */
public class ParallelStreamP<T> extends AbstractProcessor {

    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long periodNanos;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private int globalProcessorIndex;
    private long startNanoTime;
    private long totalParallelism;

    private long nowNanoTime;

    private long sequence;
    private Traverser<Object> traverser = new AppendableTraverser<>(2);

    private final List<? extends GeneratorFunction<T>> generators;
    private List<GeneratorFunction<T>> assignedGenerators;

    private ILogger logger;

    /**
     * Creates a processor that generates items using its assigned
     * generator functions. This processor picks its generator functions
     * according to its global processor index.
     *
     * @param eventsPerSecondPerGenerator the desired event rate for each generator
     * @param eventTimePolicy parameters for watermark generation
     * @param generators list of generator functions used in source
     */
    public ParallelStreamP(long eventsPerSecondPerGenerator, EventTimePolicy<? super T> eventTimePolicy,
                    List<? extends GeneratorFunction<T>> generators) {
        this.startNanoTime = System.currentTimeMillis(); // temporarily holds the parameter value until init
        this.periodNanos = NANOS_PER_SECOND / eventsPerSecondPerGenerator;
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(generators.size());
        this.generators = generators;
    }

    @Override
    protected void init(@Nonnull Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        startNanoTime = MILLISECONDS.toNanos(startNanoTime + nanoTimeMillisToCurrentTimeMillis) +
                globalProcessorIndex * periodNanos;
        // Assigned generator
        assignedGenerators = IntStream.range(0, generators.size())
                .filter(i -> i % totalParallelism == globalProcessorIndex)
                .mapToObj(generators::get)
                .collect(toList());

        logger = context.logger();
    }

    @Override
    public boolean complete() {
        nowNanoTime = System.nanoTime();
        try {
            emitEvents();
        } catch (Exception e) {
            logger.severe(e);
        }
        return false;
    }

    private void emitEvents() throws Exception {
        long emitUpTo = (nowNanoTime - startNanoTime) / periodNanos;
        while (emitFromTraverser(traverser) && sequence < emitUpTo) {
            long timestampNanoTime = startNanoTime + sequence * periodNanos;
            long timestamp = NANOSECONDS.toMillis(timestampNanoTime) - nanoTimeMillisToCurrentTimeMillis;
            for (GeneratorFunction<? extends T> generator : assignedGenerators) {
                T item = generator.generate(timestamp, sequence);
                traverser = eventTimeMapper.flatMapEvent(nowNanoTime, item, globalProcessorIndex, timestamp);
            }
            sequence++;
        }
    }

    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }

}
