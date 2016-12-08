/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;

public final class Util {

    public static final int BUFFER_SIZE = 1 << 15;

    private Util() {
    }

    public static <T> T uncheckedGet(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    public static RuntimeException unchecked(Throwable e) {
        Throwable peeled = peel(e);
        if (peeled instanceof RuntimeException) {
            return (RuntimeException) peeled;
        } else {
            return new HazelcastException(peeled);
        }
    }

    public static <T> T uncheckCall(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public static void uncheckRun(RunnableExc r) {
        try {
            r.run();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public interface RunnableExc {
        void run() throws Exception;
    }

    public static Throwable peel(Throwable e) {
        if (e instanceof CompletionException || e instanceof ExecutionException) {
            return peel(e.getCause());
        }
        return e;
    }

    @SuppressWarnings("unchecked")
    static <T extends Throwable> RuntimeException sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

    public static List<Address> getRemoteMembers(NodeEngine engine) {
        final Member localMember = engine.getLocalMember();
        return engine.getClusterService().getMembers().stream()
                     .filter(m -> !m.equals(localMember))
                     .map(Member::getAddress)
                     .collect(toList());
    }

    public static Connection getMemberConnection(NodeEngine engine, Address memberAddr) {
        return ((NodeEngineImpl) engine).getNode().getConnectionManager().getConnection(memberAddr);
    }

    public static BufferObjectDataOutput createObjectDataOutput(NodeEngine engine) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataOutput(BUFFER_SIZE);
    }

    public static BufferObjectDataInput createObjectDataInput(NodeEngine engine, byte[] buf) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataInput(buf);
    }

    public static byte[] read(InputStream in, int count) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[count];
            out.write(b, 0, in.read(b));
            return out.toByteArray();
        }
    }

    public static byte[] read(InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[BUFFER_SIZE];
            for (int len; (len = in.read(b)) != -1; ) {
                out.write(b, 0, len);
            }
            return out.toByteArray();
        }
    }

    public static void writeList(ObjectDataOutput output, List list) throws IOException {
        output.writeInt(list.size());
        for (Object o : list) {
            output.writeObject(o);
        }
    }

    public static <E> List<E> readList(ObjectDataInput output) throws IOException {
        int length = output.readInt();
        List<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(output.readObject());
        }
        return list;
    }
}
