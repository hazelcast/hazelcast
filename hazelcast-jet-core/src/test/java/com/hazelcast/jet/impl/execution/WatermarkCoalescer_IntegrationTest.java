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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.TestProcessors.MapWatermarksToString;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category(ParallelTest.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class WatermarkCoalescer_IntegrationTest extends JetTestSupport {

    @Parameter
    public Mode mode;

    private DAG dag = new DAG();
    private JetInstance instance;
    private IList<Object> sinkList;

    private enum Mode {
        TWO_EDGES,
        TWO_QUEUES
    }

    @Parameters(name = "{0}")
    public static Object[] parameters() {
        return Mode.values();
    }

    @Before
    public void before() {
        instance = super.createJetMember();
        sinkList = instance.getHazelcastInstance().getList("sinkList");
    }

    @After
    public void after() {
        super.shutdownFactory();
    }

    private static DAG createDag(Mode mode, List<Object> input1, List<Object> input2) {
        DAG dag = new DAG();

        Vertex mapWmToString = dag.newVertex("mapWmToString", MapWatermarksToString::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("sinkList")).localParallelism(1);
        dag.edge(between(mapWmToString, sink));

        switch (mode) {
            case TWO_EDGES:
                Vertex edge1 = dag.newVertex("edge1", ListSource.supplier(input1)).localParallelism(1);
                Vertex edge2 = dag.newVertex("edge2", ListSource.supplier(input2)).localParallelism(1);
                dag.edge(from(edge1).to(mapWmToString, 0));
                dag.edge(from(edge2).to(mapWmToString, 1));
                break;

            case TWO_QUEUES:
                Vertex edge = dag.newVertex("edge", ListSource.supplier(input1, input2))
                                 .localParallelism(2);
                dag.edge(between(edge, mapWmToString));
                break;

            default:
                throw new IllegalArgumentException(mode.toString());
        }

        return dag;
    }

    @Test
    public void when_i1_active_i2_active_then_wmForwardedImmediately() {
        dag = createDag(mode, singletonList(wm(100)), singletonList(wm(100)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals("wm(100)", sinkList.get(0));
    }

    @Test
    public void when_i1_active_i2_activeNoWm_then_wmForwardedAfterDelay() {
        dag = createDag(mode, singletonList(wm(100)), singletonList(entry(1, 1)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals(entry(1, 1), sinkList.get(0));
        long time = System.nanoTime();

        assertTrueEventually(() -> assertEquals(2, sinkList.size()), 6);
        assertEquals("wm(100)", sinkList.get(1));
        long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - time);
        assertTrue("Too little elapsed time, WM probably emitted immediately", elapsedMs > 3000);
    }

    @Test
    public void when_i1_activeNoWm_i2_active_then_wmForwardedAfterDelay() {
        dag = createDag(mode, singletonList(entry(1, 1)), singletonList(wm(100)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals(entry(1, 1), sinkList.get(0));
        long time = System.nanoTime();

        assertTrueEventually(() -> assertEquals(2, sinkList.size()), 6);
        assertEquals("wm(100)", sinkList.get(1));
        long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - time);
        assertTrue("Too little elapsed time, WM probably emitted immediately", elapsedMs > 3000);
    }

    @Test
    public void when_i1_active_i2_idle_then_wmForwardedImmediately() {
        dag = createDag(mode, singletonList(wm(100)), singletonList(IDLE_MESSAGE));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals("wm(100)", sinkList.get(0));
    }

    @Test
    public void when_i1_idle_i2_active_then_wmForwardedImmediately() {
        dag = createDag(mode, singletonList(IDLE_MESSAGE), singletonList(wm(100)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals("wm(100)", sinkList.get(0));
    }

    @Test
    public void when_i1_activeNoWm_i2_activeNoWm_then_wmForwardedAfterDelay() {
        dag = createDag(mode, singletonList(wm(100)), singletonList(wm(150)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals("wm(100)", sinkList.get(0));
        long time = System.nanoTime();

        assertTrueEventually(() -> assertEquals(2, sinkList.size()), 6);
        assertEquals("wm(150)", sinkList.get(1));
        long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - time);
        assertTrue("Too little elapsed time, WM probably emitted immediately", elapsedMs > 3000);
    }

    @Test
    public void when_i1_activeNoWm_i2_aheadAndIdle_then_wmForwardedAfterADelay() {
        dag = createDag(mode, singletonList(entry(1, 1)), asList(wm(150), IDLE_MESSAGE));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals(entry(1, 1), sinkList.get(0));
        long time = System.nanoTime();

        assertTrueEventually(() -> assertEquals(2, sinkList.size()), 6);
        assertEquals("wm(150)", sinkList.get(1));
        long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - time);
        assertTrue("Too little elapsed time, WM probably emitted immediately", elapsedMs > 3000);
    }

    @Test
    public void when_i1_aheadAndIdle_i2_activeNoWm_then_wmForwardedAfterADelay() {
        dag = createDag(mode, asList(wm(150), IDLE_MESSAGE), singletonList(entry(1, 1)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals(entry(1, 1), sinkList.get(0));
        long time = System.nanoTime();

        assertTrueEventually(() -> assertEquals(2, sinkList.size()), 6);
        assertEquals("wm(150)", sinkList.get(1));
        long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - time);
        assertTrue("Too little elapsed time, WM probably emitted immediately", elapsedMs > 3000);
    }

    @Test
    public void when_i1_idle_i2_idle_then_idleMessageForwardedImmediately() {
        dag = createDag(mode, singletonList(IDLE_MESSAGE), singletonList(IDLE_MESSAGE));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals("wm(" + IDLE_MESSAGE.timestamp() + ")", sinkList.get(0));
    }

    @Test
    public void when_waitingForWmOnI2ButI2BecomesDone_then_wmFromI1Forwarded() {
        dag = createDag(mode, singletonList(wm(100)), asList(delay(500), DONE_ITEM));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(1, sinkList.size()), 3);
        assertEquals("wm(100)", sinkList.get(0));
    }

    @Test
    public void when_multipleWm_then_allForwarded() {
        dag = createDag(mode, asList(wm(100), delay(500), wm(101)), asList(wm(100), delay(500), wm(101)));

        JobConfig config = new JobConfig().setMaxWatermarkRetainMillis(5000);
        instance.newJob(dag, config);

        assertTrueEventually(() -> assertEquals(2, sinkList.size()), 3);
        assertEquals("wm(100)", sinkList.get(0));
        assertEquals("wm(101)", sinkList.get(1));
    }

    private Watermark wm(long ts) {
        return new Watermark(ts);
    }

    private ListSource.Delay delay(long ms) {
        return new ListSource.Delay(ms);
    }

    /**
     * A processor that emits the given list of items.
     * The list can contain special items:<ul>
     *     <li>{@link SerializableWm} - will be emitted as a normal Watermark
     *     <li>{@link DoneItem#DONE_ITEM} - will cause the source to complete and ignore
     *          the rest of items. If this item is not present, it will never complete.
     *     <li>{@link Delay} - will cause the next item to be emitted after the delay
     * </ul>
     */
    public static class ListSource extends AbstractProcessor {
        private final List<?> list;
        private int pos;
        private long nextItemAt = Long.MIN_VALUE;

        public ListSource(List<?> list) {
            this.list = list;
        }

        @Override
        public boolean complete() {
            if (nextItemAt != Long.MIN_VALUE && System.nanoTime() < nextItemAt) {
                return false;
            }
            nextItemAt = Long.MIN_VALUE;

            while (pos < list.size()) {
                Object item = list.get(pos);
                if (item instanceof SerializableWm) {
                    item = new Watermark(((SerializableWm) item).timestamp);
                } else if (item instanceof Delay) {
                    getLogger().info("will wait " + MILLISECONDS.toNanos(((Delay) item).millis) + " ms");
                    nextItemAt = System.nanoTime() + MILLISECONDS.toNanos(((Delay) item).millis);
                    pos++;
                    return false;
                } else if (item == DONE_ITEM) {
                    getLogger().info("returning true");
                    return true;
                }

                if (tryEmit(item)) {
                    getLogger().info("emitted " + item);
                    pos++;
                } else {
                    break;
                }
            }
            return false;
        }

        /**
         * Returns meta-supplier with default local parallelism of 1, which will
         * return the same items from all instances.
         */
        public static ProcessorMetaSupplier supplier(List<Object> list) {
            List<Object> listClone = replaceWatermarks(list);
            return dontParallelize(() -> new ListSource(listClone));
        }

        /**
         * Returns meta-supplier with default local parallelism of 1, which will
         * return lists[0] from first processor, lists[1] from the second etc.
         * The number of lists and number of requested processors must match.
         */
        public static ProcessorMetaSupplier supplier(List<Object> ... lists) {
            for (int i = 0; i < lists.length; i++) {
                lists[i] = replaceWatermarks(lists[i]);
            }
            return dontParallelize(count -> Arrays.stream(lists).map(ListSource::new).collect(toList()));
        }

        // return a new list with non-serializable Watermark objects replaced.
        private static List<Object> replaceWatermarks(List<Object> list) {
            List<Object> result = new ArrayList<>();
            for (Object o : list) {
                result.add(o instanceof Watermark ? new SerializableWm(((Watermark) o).timestamp()) : o);
            }
            return result;
        }

        private static final class SerializableWm implements Serializable {
            final long timestamp;

            private SerializableWm(long timestamp) {
                this.timestamp = timestamp;
            }

            Watermark toRealWm() {
                return new Watermark(timestamp);
            }
        }

        private static final class Delay implements Serializable {
            final long millis;

            private Delay(long millis) {
                this.millis = millis;
            }
        }
    }
}
