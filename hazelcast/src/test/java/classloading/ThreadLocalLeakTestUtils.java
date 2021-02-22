/*
 * Original work Copyright 1999-2017 The Apache Software Foundation
 * Modified work Copyright (c) 2017-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import static java.lang.String.format;
import static org.junit.Assert.fail;

/**
 * Contains detection logic for {@link ThreadLocal} leaks.
 * <p>
 * Adapted from the WebappClassLoader of the Apache Tomcat project.
 *
 * @see <a href="https://github.com/apache/tomcat/blob/811450a84ca29e38d42e041be85b2deed4058ebb/java/org/apache/catalina/loader/WebappClassLoaderBase.java#L1823">WebappClassLoaderBase.java</a>
 */
public final class ThreadLocalLeakTestUtils {

    /**
     * Defines a list of value types which are explicitly being allowed to be detected in a {@link ThreadLocal}.
     */
    private static final String[] ACCEPTED_THREAD_LOCAL_VALUE_TYPES = new String[]{
            "org.mockito.configuration.DefaultMockitoConfiguration",
            "org.mockito.internal.progress.MockingProgressImpl",
    };

    private ThreadLocalLeakTestUtils() {
    }

    public static void checkThreadLocalsForLeaks(ClassLoader cl) throws Exception {
        Thread[] threads = getThreads();
        // make the fields in the Thread class that store ThreadLocals accessible
        Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
        threadLocalsField.setAccessible(true);
        Field inheritableThreadLocalsField = Thread.class.getDeclaredField("inheritableThreadLocals");
        inheritableThreadLocalsField.setAccessible(true);
        // make the underlying array of ThreadLoad.ThreadLocalMap.Entry objects accessible
        Class<?> tlmClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
        Field tableField = tlmClass.getDeclaredField("table");
        tableField.setAccessible(true);
        Method expungeStaleEntriesMethod = tlmClass.getDeclaredMethod("expungeStaleEntries");
        expungeStaleEntriesMethod.setAccessible(true);

        for (Thread thread : threads) {
            Object threadLocalMap;
            if (thread != null) {
                // clear the first map
                threadLocalMap = threadLocalsField.get(thread);
                if (threadLocalMap != null) {
                    expungeStaleEntriesMethod.invoke(threadLocalMap);
                    checkThreadLocalMapForLeaks(cl, thread, threadLocalMap, tableField);
                }

                // clear the second map
                threadLocalMap = inheritableThreadLocalsField.get(thread);
                if (threadLocalMap != null) {
                    expungeStaleEntriesMethod.invoke(threadLocalMap);
                    checkThreadLocalMapForLeaks(cl, thread, threadLocalMap, tableField);
                }
            }
        }
    }

    /**
     * Get the set of current threads as an array.
     */
    private static Thread[] getThreads() {
        // find the root thread group
        ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        try {
            while (threadGroup.getParent() != null) {
                threadGroup = threadGroup.getParent();
            }
        } catch (SecurityException se) {
            fail(format("Unable to obtain the parent for ThreadGroup [%s]. It will not be possible to check all threads"
                    + " for potential memory leaks [%s]", threadGroup.getName(), se.getMessage()));
        }

        int threadCountGuess = threadGroup.activeCount() + 50;
        Thread[] threads = new Thread[threadCountGuess];
        int threadCountActual = threadGroup.enumerate(threads);
        // make sure we don't miss any threads
        while (threadCountActual == threadCountGuess) {
            threadCountGuess *= 2;
            threads = new Thread[threadCountGuess];
            // note threadGroup.enumerate(Thread[]) silently ignores any threads that can't fit into the array
            threadCountActual = threadGroup.enumerate(threads);
        }

        return threads;
    }

    /**
     * Analyzes the given thread local map object. Also pass in the field that points
     * to the internal table to save re-calculating it on every call to this method.
     */
    private static void checkThreadLocalMapForLeaks(ClassLoader cl, Thread thread,
                                                    Object map, Field internalTableField) throws Exception {
        if (map == null) {
            return;
        }
        Object[] table = (Object[]) internalTableField.get(map);
        if (table == null) {
            return;
        }
        for (Object obj : table) {
            if (obj == null) {
                continue;
            }
            boolean keyLoadedByApplication = false;
            boolean valueLoadedByApplication = false;
            // check the key
            Object key = ((Reference<?>) obj).get();
            if (cl.equals(key) || loadedByThisOrChild(key, cl)) {
                keyLoadedByApplication = true;
            }
            // check the value
            Field valueField = obj.getClass().getDeclaredField("value");
            valueField.setAccessible(true);
            Object value = valueField.get(obj);
            if (cl.equals(value) || loadedByThisOrChild(value, cl)) {
                valueLoadedByApplication = true;
            }
            if (keyLoadedByApplication || valueLoadedByApplication) {
                Object[] args = new Object[4];
                if (key != null) {
                    args[0] = getPrettyClassName(key.getClass());
                    try {
                        args[1] = key.toString();
                    } catch (Exception e) {
                        System.err.printf("Unable to determine string representation of key of type [%s]", args[0]);
                        args[1] = "unknown";
                    }
                }
                if (value != null) {
                    args[2] = getPrettyClassName(value.getClass());
                    try {
                        args[3] = value.toString();
                    } catch (Exception e) {
                        System.err.printf("Unable to determine string representation of value of type [%s]", args[2]);
                        args[3] = "unknown";
                    }
                }
                if (valueLoadedByApplication) {
                    String message = format("Application created a ThreadLocal on thread %s with key of"
                                    + " type [%s] (value [%s]) and a value of type [%s] (value [%s) but failed"
                                    + " to remove it when the application was stopped.",
                            describeThread(thread), args[0], args[1], args[2], args[3]);
                    for (String acceptedThreadLocal : ACCEPTED_THREAD_LOCAL_VALUE_TYPES) {
                        if (acceptedThreadLocal.equals(args[2])) {
                            System.out.println(message + " But the value type is explicitly allowed, so this is no failure.");
                            return;
                        }
                    }
                    message = checkKnownIssues(message, thread, key);
                    fail(message);
                } else if (value == null) {
                    System.out.printf("Application created a ThreadLocal on thread %s with key of type [%s]"
                            + " (value [%s]). The ThreadLocal has been correctly set to null and the key will be removed by GC.",
                            describeThread(thread), args[0], args[1]);
                } else {
                    System.out.printf("Application created a ThreadLocal on thread %s with key of type [%s] (value [%s])"
                            + " and a value of type [%s] (value [%s]). Since keys are only weakly held by the"
                            + " ThreadLocalMap this is not a memory leak.",
                            describeThread(thread), args[0], args[1], args[2], args[3]);
                }
            }
        }
    }

    private static String getPrettyClassName(Class<?> clazz) {
        String name = clazz.getCanonicalName();
        if (name == null) {
            name = clazz.getName();
        }
        return name;
    }

    /**
     * @param o object to test, may be null
     * @return <code>true</code> if o has been loaded by the current classloader or one of its descendants.
     */
    private static boolean loadedByThisOrChild(Object o, ClassLoader cl) {
        if (o == null) {
            return false;
        }

        Class<?> clazz;
        if (o instanceof Class) {
            clazz = (Class<?>) o;
        } else {
            clazz = o.getClass();
        }

        ClassLoader clazzClassloader = clazz.getClassLoader();
        while (clazzClassloader != null) {
            if (clazzClassloader == cl) {
                return true;
            }
            clazzClassloader = clazzClassloader.getParent();
        }

        if (o instanceof Collection<?>) {
            Iterator<?> iter = ((Collection<?>) o).iterator();
            try {
                while (iter.hasNext()) {
                    Object entry = iter.next();
                    if (loadedByThisOrChild(entry, cl)) {
                        return true;
                    }
                }
            } catch (ConcurrentModificationException e) {
                fail(format("Failed to fully check the entries in an instance of [%s] for potential memory leaks",
                        clazz.getName()));
            }
        }
        return false;
    }

    private static String describeThread(Thread thread) {
        return format("[%s] of type [%s]", thread.getName(), getPrettyClassName(thread.getClass()));
    }

    // enhance message with information about known thread locals issues
    private static String checkKnownIssues(String message, Thread thread, Object key) {
        if (thread != null && key != null) {
            String threadClassName = getPrettyClassName(thread.getClass());
            String keyClassName = getPrettyClassName(key.getClass());
            if (threadClassName.equals("com.hazelcast.internal.networking.nio.NioThread")
                    && keyClassName.contains("ClientResponseHandlerSupplier$1")) {
                // ClientResponseHandlerSupplier$AsyncMultiThreadedResponseHandler creates thread local MutableIntegers
                // on NioThreads. During shutdown, NioThreads are interrupted but not joined in NioNetworking#shutdown
                // to avoid delay -> rarely the thread local leak detection may execute before a NioThread was joined
                // and fail the test.
                return format("%s - consider that this failure may be due to NioNetworking#shutdown not waiting for"
                        + " NioThread instances to join.", message);
            }
        }

        return message;
    }
}
