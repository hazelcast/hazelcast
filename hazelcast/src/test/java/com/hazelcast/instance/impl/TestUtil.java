/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.client.Client;
import com.hazelcast.cluster.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.impl.IntervalFunctions;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.starter.HazelcastStarter;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assert.fail;

@SuppressWarnings("WeakerAccess")
public final class TestUtil {

    private static final SerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();

    private TestUtil() {
    }

    /**
     * Serializes the given object with a default {@link SerializationService}.
     *
     * @param obj the object to serialize
     * @return the serialized {@link Data} instance
     */
    public static Data toData(Object obj) {
        return SERIALIZATION_SERVICE.toData(obj);
    }

    /**
     * Deserializes the given {@link Data} instance with a default {@link SerializationService}.
     *
     * @param data the {@link Data} instance to deserialize
     * @return the deserialized object
     */
    public static Object toObject(Data data) {
        return SERIALIZATION_SERVICE.toObject(data);
    }

    /**
     * Retrieves the {@link Node} from a given Hazelcast instance.
     *
     * @param hz the Hazelcast instance to retrieve the Node from
     * @return the {@link Node} from the given Hazelcast instance
     */
    public static Node getNode(HazelcastInstance hz) {
        if (isProxyClass(hz.getClass())) {
            return HazelcastStarter.getNode(hz);
        } else {
            HazelcastInstanceImpl hazelcastInstanceImpl = getHazelcastInstanceImpl(hz);
            return hazelcastInstanceImpl.node;
        }
    }

    /**
     * Retrieves the {@link HazelcastInstanceImpl} from a given Hazelcast instance.
     *
     * @param hz the Hazelcast instance to retrieve the implementation from
     * @return the {@link HazelcastInstanceImpl} of the given Hazelcast instance
     * @throws IllegalArgumentException if the {@link HazelcastInstanceImpl} could not be retrieved,
     *                                  e.g. when called with a Hazelcast client instance
     */
    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        if (hz instanceof HazelcastInstanceImpl impl) {
            return impl;
        } else if (hz instanceof HazelcastInstanceProxy proxy) {
            if (proxy.original != null) {
                return proxy.original;
            }
        }
        Class<? extends HazelcastInstance> clazz = hz.getClass();
        if (isProxyClass(clazz)) {
            return HazelcastStarter.getHazelcastInstanceImpl(hz);
        }
        throw new IllegalArgumentException("The given HazelcastInstance is not an active HazelcastInstanceImpl: " + clazz);
    }

    /**
     * Terminates the given Hazelcast instance.
     *
     * @param hz the instance to terminate
     */
    public static void terminateInstance(HazelcastInstance hz) {
        hz.getLifecycleService().terminate();
    }

    /**
     * Assigns the partition owners of the given Hazelcast instances.
     *
     * @param instances the given Hazelcast instances
     */
    public static void warmUpPartitions(HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            Endpoint localEndpoint = instance.getLocalEndpoint();
            if (!(localEndpoint instanceof Client)) {
                //trigger partition table arrangement and wait on members first
                warmupPartitions(instance);
            }
        }

        //wait on clients to get partition tables.
        for (HazelcastInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            if (instance.getLocalEndpoint() instanceof Client) {
                warmupPartitions(instance);
            }
        }
    }

    /**
     * Assigns the partition owners of the given Hazelcast instances.
     *
     * @param instances the given Hazelcast instances
     */
    public static void warmUpPartitions(Collection<HazelcastInstance> instances) {
        warmUpPartitions(instances.toArray(new HazelcastInstance[0]));
    }

    private static void warmupPartitions(HazelcastInstance instance) {
        if (instance == null) {
            return;
        }
        final int maxRetryCount = 15;
        IntervalFunction intervalFunction = IntervalFunctions.exponentialBackoffWithCap(10L, 2, 1000L);
        PartitionService ps = instance.getPartitionService();

        for (Partition partition : ps.getPartitions()) {
            int i = 1;
            while (partition.getOwner() == null) {
                if (i > maxRetryCount) {
                    fail("The owner of Partition{partitionId=" + partition.getPartitionId() + "}"
                            + " could not be obtained after " + maxRetryCount + " retries.");
                }
                sleepMillis((int) intervalFunction.waitAfterAttempt(i));
                ++i;
            }
        }
    }

    /**
     * Sets up current thread to be able to support User Code Namespaces for given instance.
     * Useful when namespace-aware methods are invoked not on partition threads
     * but directly in test thread. Should not be invoked from Hazelcast instance threads
     * which should have this setup done automatically.
     *
     * @implNote This method is {@link com.hazelcast.test.annotation.CompatibilityTest}-aware and sets up
     *           UCN thread locals from appropriate classloader.
     *
     * @param hz the Hazelcast instance
     */
    public static void setupNamespacesForCurrentThread(HazelcastInstance hz) {
        assert !(Thread.currentThread() instanceof HazelcastManagedThread) : "Should not be invoked on Hazelcast threads";

        if (isProxyClass(hz.getClass())) {
            // This is a shortcut.
            // Currently, onThreadStart only sets up the NodeEngineThreadLocalContext which is exactly what we need.
            // If it starts doing too much, NodeEngineThreadLocalContext.declareNodeEngineReference
            // from appropriate HazelcastAPIDelegatingClassloader will have to be invoked reflectively.
            HazelcastStarter.getNode(hz).getNodeExtension().onThreadStart(Thread.currentThread());
        } else {
            NodeEngineThreadLocalContext.declareNodeEngineReference(getNode(hz).getNodeEngine());
        }

    }

    /**
     * Returns the next available port (starting from a base port).
     *
     * @param basePort the starting port
     * @return the next available port
     * @see #isPortAvailable(int)
     */
    public static int getAvailablePort(int basePort) {
        return getAvailablePorts(basePort, 1).get(0);
    }

    /**
     * Returns a list of available ports (starting from a base port).
     *
     * @param basePort  the starting port
     * @param portCount the number of ports to search
     * @return a list of available ports
     * @see #isPortAvailable(int)
     */
    public static List<Integer> getAvailablePorts(int basePort, int portCount) {
        List<Integer> availablePorts = new ArrayList<>();
        int port = basePort;
        for (int i = 0; i < portCount; i++) {
            while (!isPortAvailable(port)) {
                port++;
            }
            availablePorts.add(port++);
        }
        return availablePorts;
    }

    /**
     * Checks the DatagramSocket as well if the port is available in UDP and TCP.
     *
     * @param port the port to check
     * @return {@code true} if the port is available in UDP and TCP, {@code false} otherwise
     */
    public static boolean isPortAvailable(int port) {
        try (ServerSocket ss = new ServerSocket(port)) {
            ss.setReuseAddress(true);

            try (DatagramSocket ds = new DatagramSocket(port)) {
                ds.setReuseAddress(true);
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Sets or removes a system property.
     * <p>
     * When the value is {@code null}, the system property will be removed.
     * <p>
     * Avoids a {@link NullPointerException} when calling {@link System#setProperty(String, String)} with a {@code null} value.
     *
     * @param key   property name
     * @param value property value
     * @return the previous string value of the system property, or {@code null} if it did not have one
     */
    public static String setSystemProperty(String key, String value) {
        return value == null ? System.clearProperty(key) : System.setProperty(key, value);
    }

    public static byte[] byteBufferToBytes(ByteBuffer buffer) {
        buffer.flip();
        byte[] requestBytes = new byte[buffer.limit()];
        buffer.get(requestBytes);
        return requestBytes;
    }
}
