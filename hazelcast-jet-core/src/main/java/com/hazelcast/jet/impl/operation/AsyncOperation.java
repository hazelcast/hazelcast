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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public abstract class AsyncOperation extends Operation implements IdentifiedDataSerializable {

    @Override
    public void beforeRun() {
        JetService service = getService();
        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
        }
    }

    protected abstract void doRun() throws Exception;

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    final void doSendResponse(Object value) {
        try {
            sendResponse(value);
        } finally {
            final JetService service = getService();
            service.getLiveOperationRegistry().deregister(this);
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isRestartableException(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public final int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

}
