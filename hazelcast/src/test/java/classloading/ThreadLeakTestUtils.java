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

public final class ThreadLeakTestUtils {

    private static final int ASSERT_TIMEOUT_SECONDS = 300;

    private static final ILogger LOGGER = Logger.getLogger(ThreadLeakTestUtils.class);

    static {
        LOGGER.info("Initializing Logger (required for thread leak tests).");
    }

    /**
     * List of whitelisted classes of threads, which are allowed to be not joinable.
     * We should not add classes of Hazelcast production code here, just test related classes.
     */
    private static final List<Class> THREAD_CLASS_WHITELIST = asList(new Class[]{
            JitterThread.class
    });

    public static Set<Thread> getThreads() {
        return Thread.getAllStackTraces().keySet();
    }

    public static void assertHazelcastThreadShutdown(Set<Thread> oldThreads) {
        Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
        Thread[] joinableThreads = getJoinableThreads(oldThreads, stackTraces.keySet());
        if (joinableThreads.length == 0) {
            return;
        }

        StringBuilder sb = new StringBuilder("There are still Hazelcast threads running after shutdown!\n");
        for (Thread thread : joinableThreads) {
            String stackTrace = Arrays.toString(stackTraces.get(thread));
            sb.append(format("-> %s (id: %s) (group: %s) (daemon: %b) (alive: %b) (interrupted: %b) (state: %s)%n%s%n%n",
                    thread.getName(), thread.getId(), getThreadGroupName(thread), thread.isDaemon(), thread.isAlive(),
                    thread.isInterrupted(), thread.getState(), stackTrace));
        }
        System.err.println(sb.toString());

        assertJoinable(ASSERT_TIMEOUT_SECONDS, joinableThreads);
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
            if (THREAD_CLASS_WHITELIST.contains(thread.getClass())) {
                iterator.remove();
            }
        }
    }

    private static String getThreadGroupName(Thread thread) {
        ThreadGroup threadGroup = thread.getThreadGroup();
        return (threadGroup == null) ? "stopped" : threadGroup.getName();
    }
}
