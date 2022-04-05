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

package com.hazelcast.jet.impl.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConfigXmlGenerator;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public final class ImdgUtil {

    private ImdgUtil() {
    }

    public static <K, V> EntryProcessor<K, V, V> entryProcessor(
            BiFunctionEx<? super K, ? super V, ? extends V> remappingFunction
    ) {
        return entry -> {
            V newValue = remappingFunction.apply(entry.getKey(), entry.getValue());
            entry.setValue(newValue);
            return newValue;
        };
    }

    public static boolean isMemberInstance(HazelcastInstance instance) {
        return instance.getLocalEndpoint() instanceof Member;
    }

    /**
     * Converts {@link ClientConfig} to xml representation using {@link
     * ClientConfigXmlGenerator}.
     */
    public static String asXmlString(ClientConfig clientConfig) {
        return clientConfig == null ? null : ClientConfigXmlGenerator.generate(clientConfig);
    }

    /**
     * Converts client-config xml string to {@link ClientConfig} using {@link
     * XmlClientConfigBuilder}.
     */
    public static ClientConfig asClientConfig(String xml) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return new XmlClientConfigBuilder(inputStream).build();
    }

    public static <T> PredicateEx<T> wrapImdgPredicate(Predicate<T> predicate) {
        return new ImdgPredicateWrapper(predicate);
    }

    public static <T> Predicate<T> maybeUnwrapImdgPredicate(PredicateEx<T> predicate) {
        if (predicate instanceof ImdgPredicateWrapper) {
            return ((ImdgPredicateWrapper<T>) predicate).wrapped;
        }
        return predicate;
    }

    public static FunctionEx wrapImdgFunction(Function function) {
        return new ImdgFunctionWrapper(function);
    }

    public static <T, R> Function<T, R> maybeUnwrapImdgFunction(FunctionEx<T, R> function) {
        if (function instanceof ImdgFunctionWrapper) {
            return ((ImdgFunctionWrapper<T, R>) function).wrapped;
        }
        return function;
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
        return ((NodeEngineImpl) engine).getNode()
                                        .getServer()
                                        .getConnectionManager(EndpointQualifier.MEMBER)
                                        .get(memberAddr);
    }

    @Nonnull
    public static BufferObjectDataOutput createObjectDataOutput(@Nonnull NodeEngine engine, int size) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataOutput(size);
    }

    @Nonnull
    public static BufferObjectDataInput createObjectDataInput(@Nonnull NodeEngine engine, @Nonnull byte[] buf) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataInput(buf);
    }

    public static void writeSubject(@Nonnull ObjectDataOutput output, @Nullable Subject subject) throws IOException {
        if (subject == null) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            output.writeBoolean(subject.isReadOnly());
            writeSet(output, subject.getPrincipals());
        }
    }

    @Nullable
    public static Subject readSubject(@Nonnull ObjectDataInput input) throws IOException {
        if (input.readBoolean()) {
            return new Subject(input.readBoolean(), readSet(input), Collections.emptySet(), Collections.emptySet());
        }
        return null;
    }

    public static void writeSet(@Nonnull ObjectDataOutput output, @Nonnull Set set) throws IOException {
        output.writeInt(set.size());
        for (Object o : set) {
            output.writeObject(o);
        }
    }

    @Nonnull
    public static <E> Set<E> readSet(@Nonnull ObjectDataInput input) throws IOException {
        int length = input.readInt();
        Set<E> set = new HashSet<>(length);
        for (int i = 0; i < length; i++) {
            set.add(input.readObject());
        }
        return set;
    }

    public static void writeList(@Nonnull ObjectDataOutput output, @Nonnull List list) throws IOException {
        output.writeInt(list.size());
        for (Object o : list) {
            output.writeObject(o);
        }
    }

    @Nonnull
    public static <E> List<E> readList(@Nonnull ObjectDataInput input) throws IOException {
        int length = input.readInt();
        List<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(input.readObject());
        }
        return list;
    }

    private static final class ImdgPredicateWrapper<T> implements PredicateEx<T> {

        private static final long serialVersionUID = 1L;

        private final Predicate<T> wrapped;

        ImdgPredicateWrapper(Predicate<T> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean testEx(T t) {
            return wrapped.test(t);
        }
    }

    private static final class ImdgFunctionWrapper<T, R> implements FunctionEx<T, R> {

        private static final long serialVersionUID = 1L;

        private final Function<T, R> wrapped;

        ImdgFunctionWrapper(Function<T, R> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public R applyEx(T t) {
            return wrapped.apply(t);
        }
    }
}
