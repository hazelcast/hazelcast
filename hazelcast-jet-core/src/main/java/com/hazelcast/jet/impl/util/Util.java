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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.JetService;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

public final class Util {

    private static final int BUFFER_SIZE = 1 << 15;

    private Util() {
    }

    public static <T> Supplier<T> memoize(Supplier<T> onceSupplier) {
        return new MemoizingSupplier<>(onceSupplier);
    }

    public static <T> Supplier<T> memoizeConcurrent(Supplier<T> onceSupplier) {
        return new ConcurrentMemoizingSupplier<T>(onceSupplier);
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
     * @param object object to check
     * @param objectName object description for the exception
     * @throws IllegalArgumentException if {@code object} is not serializable
     */
    public static void checkSerializable(Object object, String objectName) {
        if (object != null) {
            if (!(object instanceof Serializable)) {
                throw new IllegalArgumentException("\"" + objectName + "\" must be serializable");
            }
            try  (ObjectOutputStream os = new ObjectOutputStream(new NullOutputStream())) {
                os.writeObject(object);
            } catch (NotSerializableException | InvalidClassException e) {
                throw new IllegalArgumentException("\"" + objectName + "\" must be serializable", e);
            } catch (IOException e) {
                // never really thrown, as the underlying stream never throws it
                throw new JetException(e);
            }
        }
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) {
            // do nothing
        }
    }
}
