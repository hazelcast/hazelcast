/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static com.hazelcast.util.EmptyStatement.ignore;

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
        HazelcastInstanceImpl hazelcastInstanceImpl = getHazelcastInstanceImpl(hz);
        return hazelcastInstanceImpl.node;
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
        if (hz instanceof HazelcastInstanceProxy) {
            HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) hz;
            if (proxy.original != null) {
                return proxy.original;
            }
        } else if (hz instanceof HazelcastInstanceImpl) {
            return (HazelcastInstanceImpl) hz;
        }
        throw new IllegalArgumentException("Cannot retrieve HazelcastInstanceImpl from " + hz.getClass().getSimpleName());
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
            warmupPartitions(instance);
        }
    }

    /**
     * Assigns the partition owners of the given Hazelcast instances.
     *
     * @param instances the given Hazelcast instances
     */
    public static void warmUpPartitions(Collection<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            warmupPartitions(instance);
        }
    }

    private static void warmupPartitions(HazelcastInstance instance) {
        if (instance == null) {
            return;
        }
        PartitionService ps = instance.getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                sleepMillis(10);
            }
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
        List<Integer> availablePorts = new ArrayList<Integer>();
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
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            // ServerSocket is not Closeable in Java 6, so we cannot use IOUtil.closeResource() yet
            if (ds != null) {
                ds.close();
            }
            try {
                if (ss != null) {
                    ss.close();
                }
            } catch (IOException e) {
                ignore(e);
            }
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
}
