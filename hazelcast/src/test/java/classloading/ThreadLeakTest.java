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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ThreadLeakTest extends HazelcastTestSupport {

    @Test
    public void testThreadLeak() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        hz.shutdown();

        assertHazelcastThreadShutdown(threads);
    }

    public static void assertHazelcastThreadShutdown(Set<Thread> oldThreads) {
        Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
        Set<Thread> diff = stackTraces.keySet();
        diff.removeAll(oldThreads);
        if (diff.isEmpty()) {
            return;
        }

        StringBuilder sb = new StringBuilder("There are still Hazelcast threads running after shutdown!\n");
        for (Thread thread : diff) {
            String stackTrace = Arrays.toString(stackTraces.get(thread));
            sb.append(format("  -> %s (id: %s) (group: %s) (daemon: %b) (alive: %b) (interrupted: %b) (state: %s)%n%s",
                    thread.getName(), thread.getId(), getThreadGroupName(thread), thread.isDaemon(), thread.isAlive(),
                    thread.isInterrupted(), thread.getState(), stackTrace));
        }
        System.err.println(sb.toString());

        Thread[] threads = new Thread[diff.size()];
        diff.toArray(threads);

        assertJoinable(threads);
    }

    private static String getThreadGroupName(Thread thread) {
        ThreadGroup threadGroup = thread.getThreadGroup();
        return (threadGroup == null) ? "stopped" : threadGroup.getName();
    }
}
