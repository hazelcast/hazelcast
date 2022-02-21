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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.security.PermissionsUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AbstractTransformUsingServiceP<C, S> extends AbstractProcessor {

    protected final C serviceContext;
    protected final ServiceFactory<C, S> serviceFactory;

    // package-visible for test
    S service;

    public AbstractTransformUsingServiceP(@Nonnull ServiceFactory<C, S> serviceFactory, @Nullable C serviceContext) {
        this.serviceContext = serviceContext;
        this.serviceFactory = serviceFactory;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        BiFunctionEx<? super Context, ? super C, ? extends S> serviceFn = serviceFactory.createServiceFn();
        PermissionsUtil.checkPermission(serviceFn, context);
        service = serviceFn.apply(context, serviceContext);
    }

    @Override
    public boolean isCooperative() {
        return serviceFactory.isCooperative();
    }

    @Override
    public void close() throws Exception {
        if (service != null) {
            serviceFactory.destroyServiceFn().accept(service);
        }
        service = null;
    }
}
