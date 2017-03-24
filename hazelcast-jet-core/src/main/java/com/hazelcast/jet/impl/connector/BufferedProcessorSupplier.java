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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.Consumer;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static java.util.stream.Collectors.toList;

public class BufferedProcessorSupplier<B, T> implements ProcessorSupplier {

    static final long serialVersionUID = 1L;

    private final SerializableClientConfig clientConfig;
    private final Function<HazelcastInstance, Consumer<B>> consumerFunction;
    private final Supplier<B> bufferSupplier;
    private final BiConsumer<B, T> drainer;
    private final Consumer<B> bufferCloser;

    private transient Consumer<B> consumer;
    private transient HazelcastInstance client;

    BufferedProcessorSupplier(SerializableClientConfig clientConfig,
                              Supplier<B> bufferSupplier,
                              BiConsumer<B, T> drainer,
                              Function<HazelcastInstance, Consumer<B>> consumerFunction,
                              Consumer<B> bufferCloser) {
        this.clientConfig = clientConfig;
        this.consumerFunction = consumerFunction;
        this.bufferSupplier = bufferSupplier;
        this.drainer = drainer;
        this.bufferCloser = bufferCloser;
    }

    BufferedProcessorSupplier(Supplier<B> bufferSupplier,
                              BiConsumer<B, T> drainer,
                              Function<HazelcastInstance, Consumer<B>> consumerFunction,
                              Consumer<B> bufferCloser) {
        this(null, bufferSupplier, drainer, consumerFunction, bufferCloser);
    }

    @Override
    public void init(@Nonnull Context context) {
        HazelcastInstance instance;
        if (isRemote()) {
            instance = client = newHazelcastClient(clientConfig.asClientConfig());
        } else {
            instance = context.jetInstance().getHazelcastInstance();
        }
        consumer = consumerFunction.apply(instance);
    }

    @Override
    public void complete(Throwable error) {
        if (client != null) {
            client.shutdown();
        }
    }

    private boolean isRemote() {
        return clientConfig != null;
    }

    @Override @Nonnull
    public List<Processor> get(int count) {
        return Stream.generate(() -> new WriteBufferedP<>(bufferSupplier, drainer, consumer, bufferCloser))
                     .limit(count).collect(toList());
    }
}
