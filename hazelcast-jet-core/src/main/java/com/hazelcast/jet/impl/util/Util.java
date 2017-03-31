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
import com.hazelcast.jet.Distributed;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

public final class Util {

    public static final int BUFFER_SIZE = 1 << 15;

    private Util() {
    }

    public static <T> Distributed.Consumer<T> noopConsumer() {
        return t -> {
        };
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
    public static byte[] read(@Nonnull InputStream in) throws IOException {
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
}
