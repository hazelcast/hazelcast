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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.security.PermissionsUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static java.util.stream.Collectors.toList;

public abstract class AbstractHazelcastConnectorSupplier implements ProcessorSupplier {

    protected final String clientXml;

    private transient HazelcastInstance instance;
    private transient SerializationService serializationService;

    AbstractHazelcastConnectorSupplier(@Nullable String clientXml) {
        this.clientXml = clientXml;
    }

    public static ProcessorSupplier ofMap(
            @Nullable String clientXml,
            @Nonnull FunctionEx<HazelcastInstance, Processor> procFn
    ) {
        return new AbstractHazelcastConnectorSupplier(clientXml) {

            @Override
            public void init(@Nonnull Context context) {
                PermissionsUtil.checkPermission(procFn, context);
                super.init(context);
            }

            @Override
            protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
                return procFn.apply(instance);
            }
        };
    }

    @Override
    public void init(@Nonnull Context context) {
        if (clientXml != null) {
            instance = newHazelcastClient(asClientConfig(clientXml));
            serializationService = ((HazelcastClientProxy) instance).getSerializationService();
        } else {
            instance = context.hazelcastInstance();
            serializationService = ((ProcSupplierCtx) context).serializationService();
        }
    }

    @Nonnull @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(() -> createProcessor(instance, serializationService))
                     .limit(count)
                     .collect(toList());
    }

    protected abstract Processor createProcessor(HazelcastInstance instance, SerializationService serializationService);

    boolean isLocal() {
        return clientXml == null;
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (!isLocal() && instance != null) {
            instance.shutdown();
        }
    }
}
