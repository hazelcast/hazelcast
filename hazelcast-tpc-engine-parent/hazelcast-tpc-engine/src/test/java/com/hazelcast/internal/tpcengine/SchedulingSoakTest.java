package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.util.PaddedAtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.FormatUtil.humanReadableCountSI;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class SchedulingSoakTest {
    // The public properties are the tunnables for this soak test.
    public long runtimeSeconds = 500;
    // total number of reactors
    public int reactorCount = 2;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    // number of task queues per reactor
    public int taskQueueCount = 10;

    public int taskCount = 10;

    private final List<PaddedAtomicLong> counters = new ArrayList<>();
    private final MonitorThread monitorThread = new MonitorThread();
    private final List<Reactor> reactorList = new ArrayList<>();
    private final Map<Reactor, List<TaskQueue>> taskQueueMap = new HashMap<>();
    private boolean stop;

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        for (int k = 0; k < reactorCount; k++) {
            Reactor.Builder builder = newReactorBuilder();
            Reactor reactor = builder.build();
            reactorList.add(reactor);
            reactor.start();

            List<TaskQueue> taskQueues = new ArrayList<>();
            taskQueueMap.put(reactor, taskQueues);
            for (int l = 0; l < taskQueueCount; l++) {
                CompletableFuture<TaskQueue> future = reactor.submit(new Callable<TaskQueue>() {
                    @Override
                    public TaskQueue call() throws Exception {
                        //todo: play with priorities.
                        TaskQueue.Builder taskQueueBuilder = reactor.eventloop().newTaskQueueBuilder();
                        taskQueueBuilder.outside = new MpscArrayQueue<>(taskCount + 100);
                        taskQueueBuilder.inside = new CircularQueue<>(taskCount + 100);
                        return taskQueueBuilder.build();
                    }
                });

                taskQueues.add(future.join());
            }
        }
    }

    public class DummyTask extends Task {
        private final PaddedAtomicLong counter;
        private final Random random = new Random();
        private long iteration;

        public DummyTask(PaddedAtomicLong counter) {
            this.counter = counter;
        }

        @Override
        public int run() throws Throwable {
            counter.incrementAndGet();
            iteration++;
            if (iteration < 100) {
                return RUN_YIELD;
            } else {
                TaskQueue taskQueue = randomTaskQueue(random);
                if (!taskQueue.offer(this)) {
                    throw new RuntimeException("Failed to add task to taskQueue");
                }

                return RUN_COMPLETED;
            }
        }
    }

    private TaskQueue randomTaskQueue(Random random) {
        // try to find a random taskQueue we can add this task to.
        Reactor reactor = reactorList.get(random.nextInt(reactorCount));
        List<TaskQueue> taskQueueList = taskQueueMap.get(reactor);
        TaskQueue taskQueue = taskQueueList.get(random.nextInt(reactorCount));
        return taskQueue;
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactorList);
    }

    @Test
    public void test() throws Exception {
        Random random = new Random();

        for (int k = 0; k < taskCount; k++) {
            TaskQueue taskQueue = randomTaskQueue(random);
            PaddedAtomicLong counter = new PaddedAtomicLong();
            counters.add(counter);
            DummyTask task = new DummyTask(counter);
            if (!taskQueue.offer(task)) {
                throw new RuntimeException("Failed to add task to taskQueue");
            }
        }

        monitorThread.start();
        monitorThread.join();
    }

    private class DeadlineCallable implements Callable<Boolean> {
        private final PaddedAtomicLong counter;

        private DeadlineCallable(PaddedAtomicLong counter) {
            this.counter = counter;
        }

        @Override
        public Boolean call() throws Exception {
            counter.incrementAndGet();
            return true;
        }
    }

    private class MonitorThread extends Thread {
        private final StringBuffer sb = new StringBuffer();

        @Override
        public void run() {
            try {
                run0();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stop = true;
        }

        private void run0() throws InterruptedException {
            long runtimeMs = SECONDS.toMillis(runtimeSeconds);
            long startMs = currentTimeMillis();
            long endMs = startMs + runtimeMs;
            long lastMs = startMs;
            Metrics lastMetrics = new Metrics();
            Metrics metrics = new Metrics();

            while (currentTimeMillis() < endMs) {
                Thread.sleep(SECONDS.toMillis(1));
                long nowMs = currentTimeMillis();
                long durationMs = nowMs - lastMs;

                collect(metrics);

                printEtd(nowMs, startMs, runtimeMs);

                printEta(endMs, nowMs);

                printThp(metrics, lastMetrics, durationMs);

                printTaskCsThp(metrics, lastMetrics, durationMs);

                printTaskQueueCsThp(metrics, lastMetrics, durationMs);

                System.out.println(sb);
                sb.setLength(0);

                Metrics tmp = lastMetrics;
                lastMetrics = metrics;
                metrics = tmp;
                lastMs = nowMs;
            }
        }

        private void printThp(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long diff = metrics.count - lastMetrics.count;
            double thp = ((diff) * 1000d) / durationMs;
            sb.append("[thp=");
            sb.append(humanReadableCountSI(thp));
            sb.append("/s]");
        }

        private void printTaskCsThp(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long diff = metrics.taskCsCount - lastMetrics.taskCsCount;
            double thp = ((diff) * 1000d) / durationMs;
            sb.append("[task-cs=");
            sb.append(humanReadableCountSI(thp));
            sb.append("/s]");
        }

        private void printTaskQueueCsThp(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long diff = metrics.taskQueueCsCount - lastMetrics.taskQueueCsCount;
            double thp = ((diff) * 1000d) / durationMs;
            sb.append("[task-q-cs=");
            sb.append(humanReadableCountSI(thp));
            sb.append("/s]");
        }

        private void printEta(long endMs, long nowMs) {
            long eta = MILLISECONDS.toSeconds(endMs - nowMs);
            sb.append("[eta ");
            sb.append(eta / 60);
            sb.append("m:");
            sb.append(eta % 60);
            sb.append("s]");
        }

        private void printEtd(long nowMs, long startMs, long runtimeMs) {
            long completedSeconds = MILLISECONDS.toSeconds(nowMs - startMs);
            double completed = (100f * completedSeconds) / runtimeSeconds;
            sb.append("[etd ");
            sb.append(completedSeconds / 60);
            sb.append("m:");
            sb.append(completedSeconds % 60);
            sb.append("s ");
            sb.append(String.format("%,.3f", completed));
            sb.append("%]");
        }
    }

    private static long sum(List<PaddedAtomicLong> list) {
        long sum = 0;
        for (PaddedAtomicLong a : list) {
            sum += a.get();
        }
        return sum;
    }

    private void collect(Metrics target) {
        target.clear();

        for (int k = 0; k < reactorCount; k++) {
            Reactor reactor = reactorList.get(k);
            Reactor.Metrics metrics = reactor.metrics;
            target.taskCsCount += metrics.taskCsSwitchCount();
            target.taskQueueCsCount += metrics.taskCsSwitchCount();
        }

        target.count = sum(counters);
    }

    private static class Metrics {
        public long taskCsCount;
        public long taskQueueCsCount;
        private long count;

        private void clear() {
            count = 0;
            taskCsCount = 0;
            taskQueueCsCount = 0;
        }
    }
}
