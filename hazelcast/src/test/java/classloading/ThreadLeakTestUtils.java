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

package classloading;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.jitter.JitterThread;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static java.lang.String.format;
import static java.util.Arrays.asList;

@SuppressWarnings("WeakerAccess")
public final class ThreadLeakTestUtils {

    private static final int ASSERT_TIMEOUT_SECONDS = 300;

    /**
     * List of whitelisted classes of threads, which are allowed to be not joinable.
     * We should not add classes of Hazelcast production code here, just test related classes.
     */
    private static final List<Class<?>> THREAD_CLASS_WHITELIST = asList(new Class<?>[]{
            JitterThread.class,
    });
    private static final List<String> THREAD_NAME_WHITELIST = asList(
            "process reaper",
            "surefire-forkedjvm-ping-30s"
    );

    private static final ILogger LOGGER = Logger.getLogger(ThreadLeakTestUtils.class);

    static {
        LOGGER.info("Initializing Logger (required for thread leak tests)");
    }

    private ThreadLeakTestUtils() {
    }

    public static Set<Thread> getThreads() {
        return Thread.getAllStackTraces().keySet();
    }

    public static void assertHazelcastThreadShutdown(Set<Thread> oldThreads) {
        assertHazelcastThreadShutdown("There are still Hazelcast threads running after shutdown!", oldThreads);
    }

    public static void assertHazelcastThreadShutdown(String message, Set<Thread> oldThreads) {
        Thread[] joinableThreads = getAndLogThreads(message, oldThreads);
        if (joinableThreads == null) {
            return;
        }

        try {
            assertJoinable(ASSERT_TIMEOUT_SECONDS, joinableThreads);
        } catch (AssertionError e) {
            getAndLogThreads("There are threads which survived assertJoinable()!", oldThreads);
            throw e;
        }
    }

    public static Thread[] getAndLogThreads(String message, Set<Thread> oldThreads) {
        Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
        Set<Thread> nonSystemNonCommonPoolThreads = getNonSystemAndNonCommonPoolThreads(stackTraces);
        Thread[] joinableThreads = getJoinableThreads(oldThreads, nonSystemNonCommonPoolThreads);
        if (joinableThreads.length == 0) {
            return null;
        }
        logThreads(stackTraces, joinableThreads, message);
        return joinableThreads;
    }

    private static Set<Thread> getNonSystemAndNonCommonPoolThreads(Map<Thread, StackTraceElement[]> stackTraces) {
        Set<Thread> nonSystemThreads = new HashSet<>();
        for (Thread thread : stackTraces.keySet()) {
            ThreadGroup threadGroup = thread.getThreadGroup();
            if (threadGroup != null && threadGroup.getParent() != null && !thread.getName().contains("ForkJoinPool.commonPool")) {
                // non-system alive thread
                nonSystemThreads.add(thread);
            }
        }
        return nonSystemThreads;
    }

    private static Thread[] getJoinableThreads(Set<Thread> oldThreads, Set<Thread> newThreads) {
        Set<Thread> diff = new HashSet<Thread>(newThreads);
        diff.removeAll(oldThreads);
        diff.remove(Thread.currentThread());
        removeWhitelistedThreadClasses(diff);

        Thread[] joinable = new Thread[diff.size()];
        diff.toArray(joinable);
        return joinable;
    }

    private static void removeWhitelistedThreadClasses(Set<Thread> threads) {
        Iterator<Thread> iterator = threads.iterator();
        while (iterator.hasNext()) {
            Thread thread = iterator.next();
            Class<? extends Thread> threadClass = thread.getClass();
            String threadName = thread.getName();
            if (THREAD_CLASS_WHITELIST.contains(threadClass) || THREAD_NAME_WHITELIST.contains(threadName)) {
                iterator.remove();
            }
        }
    }

    private static void logThreads(Map<Thread, StackTraceElement[]> stackTraces, Thread[] threads, String message) {
        StringBuilder sb = new StringBuilder(message);
        for (Thread thread : threads) {
            String stackTrace = Arrays.toString(stackTraces.get(thread));
            sb.append(format("%n-> %s (id: %s) (group: %s) (daemon: %b) (alive: %b) (interrupted: %b) (state: %s)%n%s%n",
                    thread.getName(), thread.getId(), getThreadGroupName(thread), thread.isDaemon(), thread.isAlive(),
                    thread.isInterrupted(), thread.getState(), stackTrace));
        }
        LOGGER.severe(sb.toString());
    }

    private static String getThreadGroupName(Thread thread) {
        ThreadGroup threadGroup = thread.getThreadGroup();
        return (threadGroup == null) ? "stopped" : threadGroup.getName();
    }
}
