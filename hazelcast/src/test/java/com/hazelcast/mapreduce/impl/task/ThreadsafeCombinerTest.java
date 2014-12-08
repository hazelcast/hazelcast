/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.Assert.assertTrue;

public class ThreadsafeCombinerTest {

    /**
     * Combiner creation is not threadsafe
     *
     * @throws Exception
     */
    @Test
    public void github_issue_3625()
            throws Exception {
        class TestCombinerFactory
                implements CombinerFactory {

            @Override
            public Combiner newCombiner(Object key) {
                return new Combiner() {
                    @Override
                    public void combine(Object value) {
                    }

                    @Override
                    public Object finalizeChunk() {
                        return null;
                    }
                };
            }
        }

        class CreationTask
                implements Runnable {
            private final CountDownLatch latchStart;
            private final CountDownLatch latchEnd;
            private final AtomicReferenceArray<Combiner> array;
            private final DefaultContext<Integer, Integer> defaultContext;
            private final int index;

            CreationTask(CountDownLatch latchStart, CountDownLatch latchEnd, AtomicReferenceArray<Combiner> array,
                         DefaultContext<Integer, Integer> defaultContext, int index) {
                this.latchStart = latchStart;
                this.latchEnd = latchEnd;
                this.array = array;
                this.defaultContext = defaultContext;
                this.index = index;
            }

            @Override
            public void run() {
                try {
                    latchStart.await();
                    Combiner combiner = defaultContext.getOrCreateCombiner(1);
                    array.set(index, combiner);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latchEnd.countDown();
                }
            }
        }

        int threadCount = 20;

        AtomicReferenceArray<Combiner> combiners = new AtomicReferenceArray<Combiner>(threadCount);
        DefaultContext<Integer, Integer> context = new DefaultContext<Integer, Integer>(new TestCombinerFactory(), null);

        CountDownLatch latchStart = new CountDownLatch(1);
        CountDownLatch latchEnd = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new CreationTask(latchStart, latchEnd, combiners, context, i));
            t.start();
        }

        latchStart.countDown();
        latchEnd.await(1, TimeUnit.MINUTES);

        for (int i = 0; i < threadCount - 1; i++) {
            Combiner c1 = combiners.get(i);
            Combiner c2 = combiners.get(i + 1);
            assertTrue("Returned combiners are not identical: " + c1 + " -> " + c2, c1 == c2);
        }
    }
}
