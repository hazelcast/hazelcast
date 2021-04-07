/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.stackTraceToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

/**
 * Base class for async operations. Handles registration/deregistration of
 * operations from live registry, exception handling and peeling and
 * logging of exceptions
 */
public abstract class AsyncOperation extends Operation implements IdentifiedDataSerializable {

    @Override
    public void beforeRun() {
        JetService service = getService();
        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public final void run() {
        CompletableFuture<?> future;
        try {
            future = doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
            return;
        }
        future.whenComplete(withTryCatch(getLogger(), (r, f) -> doSendResponse(f != null ? peel(f) : r)));
    }

    protected abstract CompletableFuture<?> doRun() throws Exception;

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    private void doSendResponse(Object value) {
        try {
            final JetService service = getService();
            service.getLiveOperationRegistry().deregister(this);
        } finally {
            try {
                sendResponse(value);
            } catch (Exception e) {
                Throwable ex = peel(e);
                if (value instanceof Throwable && ex instanceof HazelcastSerializationException) {
                    // Sometimes exceptions are not serializable, for example on
                    // https://github.com/hazelcast/hazelcast-jet/issues/1995.
                    // When sending exception as a response and the serialization fails,
                    // the response will not be sent and the operation will hang.
                    // To prevent this from happening, replace the exception with
                    // another exception that can be serialized.
                    sendResponse(new JetException(stackTraceToString(ex)));
                } else {
                    throw e;
                }
            }
        }
    }

    protected JetService getJetService() {
        assert getServiceName().equals(JetService.SERVICE_NAME) : "Service is not Jet Service";
        return getService();
    }

    protected JobCoordinationService getJobCoordinationService() {
        return getJetService().getJobCoordinationService();
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
