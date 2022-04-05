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

package com.hazelcast.jet.core;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ObservableResultsTest extends TestInClusterSupport {

    private String observableName;
    private TestObserver testObserver;
    private Observable<Long> testObservable;
    private UUID registrationId;

    @Before
    public void before() {
        hz().getJet().getObservables().forEach(Observable::destroy);

        observableName = randomName();
        testObserver = new TestObserver();
        testObservable = hz().getJet().getObservable(observableName);
        registrationId = testObservable.addObserver(testObserver);
    }

    @After
    @Override
    public void after() throws Exception {
        hz().getJet().getObservables().forEach(Observable::destroy);
        super.after();
    }

    @Test
    public void iterable() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        hz().getJet().newJob(pipeline).join();
        //then
        List<Long> items = new ArrayList<>();
        for (Long item : testObservable) {
            items.add(item);
        }
        items.sort(Long::compareTo);
        assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), items);
    }

    @Test
    public void batchJobCompletesSuccessfully() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        hz().getJet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
    }

    @Test
    public void batchJobFails() {
        BatchSource<String> errorSource = SourceBuilder
                .batch("error-source", x -> null)
                .<String>fillBufferFn((in, Void) -> {
                    throw new RuntimeException("Intentionally thrown!");
                })
                .destroyFn(ConsumerEx.noop())
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(errorSource)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));
        //then
        assertSortedValues(testObserver);
        assertError(testObserver, "Intentionally thrown!");
        assertCompletions(testObserver, 0);
    }

    @Test
    public void streamJob() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.cancel();
        //then
        assertError(testObserver, "CancellationException");
        assertCompletions(testObserver, 0);
    }

    @Test
    public void streamJobRestart() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.restart();
        //then
        int resultsSoFar = testObserver.getNoOfValues();
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > resultsSoFar));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);
    }

    @Test
    public void multipleObservables() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> stage = pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L));

        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> otherObservable = hz().getJet().getObservable("otherObservable");
        otherObservable.addObserver(otherTestObserver);

        stage.filter(i -> i % 2 == 0).writeTo(Sinks.observable(observableName));
        stage.filter(i -> i % 2 != 0).writeTo(Sinks.observable("otherObservable"));

        //when
        Job job = hz().getJet().newJob(pipeline);
        job.join();
        //then
        assertSortedValues(testObserver, 0L, 2L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
        //also
        assertSortedValues(otherTestObserver, 1L, 3L);
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 1);
    }

    @Test
    public void multipleIdenticalSinks() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> readStage = pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L));
        readStage.writeTo(Sinks.observable(observableName));
        readStage.writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        job.join();
        //then
        assertSortedValues(testObserver, 0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
    }

    @Test
    public void multipleJobsWithTheSameSink() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .filter(t -> (t % 2 == 0))
                .writeTo(Sinks.observable(observableName));
        Pipeline pipeline2 = Pipeline.create();
        pipeline2.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .filter(t -> (t % 2 != 0))
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        Job job2 = hz().getJet().newJob(pipeline2);
        //then
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job2.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertTrueEventually(() -> {
            List<Long> sortedValues = testObserver.getSortedValues();
            assertEquals(0, (long) sortedValues.get(0));
            assertEquals(1, (long) sortedValues.get(1));
        });
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        job.cancel();
        job2.cancel();
    }

    @Test
    public void multipleJobExecutions() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        hz().getJet().newJob(pipeline).join();
        hz().getJet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, 0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 2);
    }

    @Test
    public void observersGetAllEventsStillInRingbuffer() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName));

        //when
        hz().getJet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);

        //when
        TestObserver otherTestObserver = new TestObserver();
        hz().getJet().<Long>getObservable(observableName).addObserver(otherTestObserver);
        //then
        assertSortedValues(otherTestObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 1);
    }

    @Test
    public void observableRegisteredAfterJobFinishedGetAllEventsStillInRingbuffer() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(observableName + "late"));

        //when
        hz().getJet().newJob(pipeline).join();
        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> lateObservable = hz().getJet().getObservable(observableName + "late");
        lateObservable.addObserver(otherTestObserver);
        //then
        assertSortedValues(otherTestObserver, 0L, 1L, 2L, 3L, 4L);
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 1);
    }

    @Test
    public void observableRegisteredAfterJobFailedGetError() {
        BatchSource<String> errorSource = SourceBuilder
                .batch("error-source", x -> null)
                .<String>fillBufferFn((in, Void) -> {
                    throw new RuntimeException("Intentionally thrown!");
                })
                .destroyFn(ConsumerEx.noop())
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(errorSource)
                .writeTo(Sinks.observable(observableName));

        Job job = hz().getJet().newJob(pipeline);
        assertTrueEventually(() -> assertEquals(JobStatus.FAILED, job.getStatus()));

        //when
        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> lateObservable = hz().getJet().getObservable(observableName);
        lateObservable.addObserver(otherTestObserver);
        //then
        assertSortedValues(testObserver);
        assertError(testObserver, "Intentionally thrown!");
        assertCompletions(testObserver, 0);
    }

    @Test
    public void errorInOneJobIsNotTerminalForOthers() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));
        Pipeline pipeline2 = Pipeline.create();
        pipeline2.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        Job job2 = hz().getJet().newJob(pipeline2);
        //then
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job2.getStatus()));
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        job.cancel();
        assertError(testObserver, "CancellationException");
        assertCompletions(testObserver, 0);

        //then - job2 is still running
        int resultsSoFar = testObserver.getNoOfValues();
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > resultsSoFar));

        job2.cancel();
    }

    @Test
    public void removedObserverDoesNotGetFurtherEvents() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName));

        //when
        Job job = hz().getJet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(testObserver.getNoOfValues() > 10));
        assertError(testObserver, null);
        assertCompletions(testObserver, 0);

        //when
        testObservable.removeObserver(registrationId);
        //then
        int resultsSoFar = testObserver.getNoOfValues();
        assertTrueAllTheTime(() -> assertTrue(testObserver.getNoOfValues() <= resultsSoFar + 1), 2);

        job.cancel();
    }

    @Test
    public void destroyedObservableDoesNotGetFurtherEvents() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence)
                .writeTo(Sinks.observable(observableName + "destroyed"));

        TestObserver otherTestObserver = new TestObserver();
        Observable<Long> destroyedObservable = hz().getJet().getObservable(observableName + "destroyed");
        destroyedObservable.addObserver(otherTestObserver);
        //when
        Job job = hz().getJet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(otherTestObserver.getNoOfValues() > 10));
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 0);

        //when
        destroyedObservable.destroy();
        //then
        int resultsSoFar = otherTestObserver.getNoOfValues();
        assertTrueAllTheTime(() -> assertTrue(otherTestObserver.getNoOfValues() <= resultsSoFar + 1), 2);
        job.cancel();
        assertError(otherTestObserver, null);
        assertCompletions(otherTestObserver, 0);
    }

    @Test
    public void fastResultsDoNotGetLost_moreThanBatchSize() {
        fastResultsDoNotGetLost(RingbufferProxy.MAX_BATCH_SIZE * 5);
    }

    @Test
    @Ignore //TODO: fast results still can get lost
    public void fastResultsDoNotGetLost_moreThanRingbufferCapacity() {
        fastResultsDoNotGetLost(250_000);
    }

    private void fastResultsDoNotGetLost(int noOfResults) {
        List<Long> sourceItems = LongStream.range(0, noOfResults)
                .boxed()
                .collect(Collectors.toList());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(sourceItems))
                .writeTo(Sinks.observable(observableName));

        //when
        hz().getJet().newJob(pipeline).join();
        //then
        assertSortedValues(testObserver, sourceItems.toArray(new Long[0]));
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
    }

    @Test
    @Ignore //TODO: fast results still can get lost
    public void fastResultsDoNotGetLost_whenUsingIterator() throws Exception {
        int noOfResults = 250_000;

        List<Long> sourceItems = LongStream.range(0, noOfResults)
                .boxed()
                .collect(Collectors.toList());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(sourceItems))
                .writeTo(Sinks.observable(observableName));

        //when
        Future<Long> stream = testObservable.toFuture(Stream::count);
        hz().getJet().newJob(pipeline);
        //then
        assertEquals(noOfResults, stream.get().longValue());
        assertError(testObserver, null);
        assertCompletions(testObserver, 1);
    }

    @Test
    public void sinkConsumesThrowables() throws Exception {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Throwable> input = TestSources.items(
                new RuntimeException("runtime_exception"),
                new Exception("exception"),
                new Error("error"),
                new Throwable("throwable")
        );
        pipeline.readFrom(input)
                .writeTo(Sinks.observable("throwables"));

        //when
        hz().getJet().newJob(pipeline).join();
        List<Object> results = hz().getJet().getObservable("throwables")
                .toFuture(s -> {
                    Comparator<Object> comparator = Comparator.comparing(o -> ((Throwable) o).getMessage());
                    return s.sorted(comparator).collect(Collectors.toList());
                })
                .get();
        //then
        assertEquals(4, results.size());
        assertEquals("error", ((Throwable) results.get(0)).getMessage());
        assertEquals("exception", ((Throwable) results.get(1)).getMessage());
        assertEquals("runtime_exception", ((Throwable) results.get(2)).getMessage());
        assertEquals("throwable", ((Throwable) results.get(3)).getMessage());
    }

    @Test
    public void configureCapacity() {
        //when
        Observable<Object> o = hz().getJet().newObservable();
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(o));
        //then
        o.configureCapacity(20_000); //still possible, pipeline not executing yet
        assertThrowsException(o::getConfiguredCapacity, IllegalStateException.class);

        //when
        Job job = hz().getJet().newJob(pipeline);
        assertExecutionStarted(job);
        //then
        assertThrowsException(() -> o.configureCapacity(30_000), IllegalStateException.class);
        assertEquals(20_000, o.getConfiguredCapacity());

        //when
        job.join();
        ///then
        assertThrowsException(() -> o.configureCapacity(30_000), IllegalStateException.class);
        assertEquals(20_000, o.getConfiguredCapacity());
    }

    @Test
    public void configureCapacityMultipleTimes() {
        Observable<Object> o = hz().getJet().newObservable();
        o.configureCapacity(10);
        assertThrowsException(() -> o.configureCapacity(20), RuntimeException.class);
    }

    @Test
    public void unnamedObservable() {
        Observable<Long> unnamedObservable = hz().getJet().newObservable();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable(unnamedObservable));

        TestObserver observer = new TestObserver();
        unnamedObservable.addObserver(observer);

        //when
        hz().getJet().newJob(pipeline).join();
        //then
        assertSortedValues(observer, 0L, 1L, 2L, 3L, 4L);
        assertError(observer, null);
        assertCompletions(observer, 1);
    }

    @Test
    public void onlyObservedObservablesGetActivated() {
        //when
        Observable<Object> a = hz().getJet().newObservable();
        Observable<Object> b = hz().getJet().newObservable();
        Observable<Object> c = hz().getJet().newObservable();

        a.addObserver(Observer.of(ConsumerEx.noop()));
        c.addObserver(Observer.of(ConsumerEx.noop()));

        //then
        assertTrueEventually(() -> {
            Set<String> observables = hz().getJet().getObservables().stream().map(Observable::name).collect(Collectors.toSet());
            assertTrue(observables.containsAll(Arrays.asList(a.name(), c.name())));
            assertFalse(observables.contains(b.name()));
        });
    }

    @Test
    public void createAndDestroyObservableRepeatedly() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .writeTo(Sinks.observable("repeatedObservable"));

        for (int i = 0; i < 20; i++) {
            TestObserver repeatedTestObserver = new TestObserver();
            Observable<Long> repeatedObservable = hz().getJet().getObservable("repeatedObservable");
            repeatedObservable.addObserver(repeatedTestObserver);
            //when
            hz().getJet().newJob(pipeline).join();
            //then
            assertSortedValues(repeatedTestObserver, 0L, 1L, 2L, 3L, 4L);
            assertError(repeatedTestObserver, null);
            assertCompletions(repeatedTestObserver, 1);
            repeatedObservable.destroy();
        }
    }

    private void assertExecutionStarted(Job job) {
        assertTrueEventually(() -> assertTrue(JobStatus.RUNNING.equals(job.getStatus())
                || JobStatus.COMPLETED.equals(job.getStatus())));
    }

    private void assertThrowsException(Runnable action, Class<? extends Throwable> exceptionClass) {
        //then
        try {
            action.run();
            fail("Expected exception not thrown");
        } catch (Throwable t) {
            assertEquals(exceptionClass, t.getClass());
        }
    }

    private static void assertSortedValues(TestObserver observer, Long... values) {
        assertTrueEventually(() -> assertEquals(Arrays.asList(values), observer.getSortedValues()));
    }

    private static void assertError(TestObserver observer, String error) {
        if (error == null) {
            assertNull(observer.getError());
        } else {
            assertTrueEventually(() -> {
                assertNotNull(observer.getError());
                assertTrue(observer.getError().toString().contains(error));
            });
        }
    }

    private static void assertCompletions(TestObserver observer, int completions) {
        assertTrueEventually(() -> assertEquals(completions, observer.getNoOfCompletions()));
    }

    private static final class TestObserver implements Observer<Long> {

        private final List<Long> values = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicInteger completions = new AtomicInteger();

        @Override
        public void onNext(@Nonnull Long value) {
            values.add(value);
        }

        @Override
        public void onError(@Nonnull Throwable throwable) {
            error.set(throwable);
        }

        @Override
        public void onComplete() {
            completions.incrementAndGet();
        }

        int getNoOfValues() {
            synchronized (values) {
                return values.size();
            }
        }

        @Nonnull
        List<Long> getSortedValues() {
            List<Long> sortedValues;
            synchronized (values) {
                sortedValues = new ArrayList<>(values);
            }
            sortedValues.sort(Long::compare);
            return sortedValues;
        }

        @Nullable
        Throwable getError() {
            return error.get();
        }

        int getNoOfCompletions() {
            return completions.get();
        }
    }

}
