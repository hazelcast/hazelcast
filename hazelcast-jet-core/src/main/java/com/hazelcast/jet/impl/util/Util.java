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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public final class Util {

    private static final int BUFFER_SIZE = 1 << 15;
    private static final char[] ID_TEMPLATE = "0000-0000-0000-0000".toCharArray();

    private Util() {
    }

    public static <T> Supplier<T> memoize(Supplier<T> onceSupplier) {
        return new MemoizingSupplier<>(onceSupplier);
    }

    public static <T> Supplier<T> memoizeConcurrent(Supplier<T> onceSupplier) {
        return new ConcurrentMemoizingSupplier<>(onceSupplier);
    }

    public static <T> T uncheckCall(@Nonnull Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public static void uncheckRun(@Nonnull RunnableExc r) {
        try {
            r.run();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * This method will generate an {@link ExecutionCallback} which
     * allows to asynchronously get notified when the execution is completed,
     * either successfully or with error by calling {@code onResponse} on success
     * and {@code onError} on error respectively.
     *
     * @param onResponse function to call when execution is completed successfully
     * @param onError function to call when execution is completed with error
     * @param <T> type of the response
     * @return {@link ExecutionCallback}
     */
    public static <T> ExecutionCallback<T> callbackOf(Consumer<T> onResponse, Consumer<Throwable> onError) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                onResponse.accept(o);
            }

            @Override
            public void onFailure(Throwable throwable) {
                onError.accept(throwable);
            }
        };
    }

    /**
     * Atomically increment the {@code value} by {@code increment}, unless
     * the value after increment would exceed the {@code limit}.
     * <p>
     *
     * @param limit maximum value the {@code value} can take (inclusive)
     * @return {@code true}, if successful, {@code false}, if {@code limit} would be exceeded.
     */
    public static boolean tryIncrement(AtomicInteger value, int increment, int limit) {
        int prev;
        int next;
        do {
            prev = value.get();
            next = prev + increment;
            if (next > limit) {
                return false;
            }
        } while (!value.compareAndSet(prev, next));
        return true;
    }

    public interface RunnableExc {
        void run() throws Exception;
    }

    @Nonnull
    public static List<Address> getRemoteMembers(@Nonnull NodeEngine engine) {
        final Member localMember = engine.getLocalMember();
        return engine.getClusterService().getMembers().stream()
                     .filter(m -> !m.equals(localMember))
                     .map(Member::getAddress)
                     .collect(toList());
    }

    public static Connection getMemberConnection(@Nonnull NodeEngine engine, @Nonnull Address memberAddr) {
        return ((NodeEngineImpl) engine).getNode().getConnectionManager().getConnection(memberAddr);
    }

    public static JetInstance getJetInstance(NodeEngine nodeEngine) {
        return nodeEngine.<JetService>getService(JetService.SERVICE_NAME).getJetInstance();
    }

    @Nonnull
    public static BufferObjectDataOutput createObjectDataOutput(@Nonnull NodeEngine engine) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataOutput(BUFFER_SIZE);
    }

    @Nonnull
    public static BufferObjectDataInput createObjectDataInput(@Nonnull NodeEngine engine, @Nonnull byte[] buf) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataInput(buf);
    }

    @Nonnull
    public static byte[] readFully(@Nonnull InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[BUFFER_SIZE];
            for (int len; (len = in.read(b)) != -1; ) {
                out.write(b, 0, len);
            }
            return out.toByteArray();
        }
    }

    public static void writeList(@Nonnull ObjectDataOutput output, @Nonnull List list) throws IOException {
        output.writeInt(list.size());
        for (Object o : list) {
            output.writeObject(o);
        }
    }

    @Nonnull
    public static <E> List<E> readList(@Nonnull ObjectDataInput output) throws IOException {
        int length = output.readInt();
        List<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(output.readObject());
        }
        return list;
    }

    public static long addClamped(long a, long b) {
        long sum = a + b;
        return sumHadOverflow(a, b, sum)
                ? (a >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE)
                : sum;
    }

    public static long subtractClamped(long a, long b) {
        long diff = a - b;
        return diffHadOverflow(a, b, diff)
                ? (a >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE)
                : diff;
    }

    // Hacker's Delight, 2nd Ed, 2-13: overflow has occurred iff
    // operands have the same sign which is opposite of the result
    public static boolean sumHadOverflow(long a, long b, long sum) {
        return ((a ^ sum) & (b ^ sum)) < 0;
    }

    // Hacker's Delight, 2nd Ed, 2-13: overflow has occurred iff operands have
    // opposite signs and result has opposite sign of left-hand operand
    public static boolean diffHadOverflow(long a, long b, long diff) {
        return ((a ^ b) & (a ^ diff)) < 0;
    }

    /**
     * Checks that the {@code object} implements {@link Serializable} and is
     * correctly serializable by actually trying to serialize it. This will
     * reveal some non-serializable field early.
     *
     * @param object     object to check
     * @param objectName object description for the exception
     * @throws IllegalArgumentException if {@code object} is not serializable
     */
    public static void checkSerializable(Object object, String objectName) {
        if (object == null) {
            return;
        }
        if (!(object instanceof Serializable)) {
            throw new IllegalArgumentException("\"" + objectName + "\" must implement Serializable");
        }
        try (ObjectOutputStream os = new ObjectOutputStream(new NullOutputStream())) {
            os.writeObject(object);
        } catch (NotSerializableException | InvalidClassException e) {
            throw new IllegalArgumentException("\"" + objectName + "\" must be serializable", e);
        } catch (IOException e) {
            // never really thrown, as the underlying stream never throws it
            throw new JetException(e);
        }
    }

    /**
     * Distributes the owned partitions to processors in a round-robin fashion
     * If owned partition size is smaller than processor count
     * an empty list is put for the rest of the processors
     * @param count count of processors
     * @param ownedPartitions list of owned partitions
     * @return a map of which has partition index as key and list of partition ids as value
     */
    public static Map<Integer, List<Integer>> processorToPartitions(int count, List<Integer> ownedPartitions) {
        Map<Integer, List<Integer>> processorToPartitions = range(0, ownedPartitions.size())
                .mapToObj(i -> entry(i, ownedPartitions.get(i)))
                .collect(groupingBy(e -> e.getKey() % count, mapping(Map.Entry::getValue, toList())));

        for (int processor = 0; processor < count; processor++) {
            processorToPartitions.computeIfAbsent(processor, x -> emptyList());
        }
        return processorToPartitions;
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) {
            // do nothing
        }
    }

    /*
 * The random number generator used by this class to create random
 * based UUIDs. In a holder class to defer initialization until needed.
 */
    private static class Holder {
        static final SecureRandom NUMBER_GENERATOR = new SecureRandom();
    }

    public static long secureRandomNextLong() {
        return Holder.NUMBER_GENERATOR.nextLong();
    }

    public static String jobAndExecutionId(long jobId, long executionId) {
        return "job " + idToString(jobId) + ", execution " + idToString(executionId);
    }

    public static ZonedDateTime toZonedDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalDateTime();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static String idToString(long id) {
        char[] buf = Arrays.copyOf(ID_TEMPLATE, ID_TEMPLATE.length);
        String hexStr = Long.toHexString(id);
        for (int i = hexStr.length() - 1, j = 18; i >= 0; i--, j--) {
            buf[j] = hexStr.charAt(i);
            if (j == 15 || j == 10 || j == 5) {
                j--;
            }
        }
        return new String(buf);
    }

    public static <K, V> EntryProcessor<K, V> entryProcessor(
            DistributedBiFunction<? super K, ? super V, ? extends V> remappingFunction
    ) {
        return new AbstractEntryProcessor<K, V>() {
            @Override
            public Object process(Entry<K, V> entry) {
                V newValue = remappingFunction.apply(entry.getKey(), entry.getValue());
                entry.setValue(newValue);
                return newValue;
            }
        };
    }

    public static <K, V> V compute(IMap<K, V> map, K key,
                                  DistributedBiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return (V) map.executeOnKey(key, entryProcessor(remappingFunction));
    }

    /**
     * Sequentially search through an array, return the index of first {@code
     * needle} element in {@code haystack} or -1, if not found.
     */
    public static int arrayIndexOf(int needle, int[] haystack) {
        for (int i = 0; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a future which is already completed with the supplied exception
     */
    public static <T> CompletableFuture<T> exceptionallyCompletedFuture(@Nonnull Throwable exception) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(exception);
        return future;
    }

}
