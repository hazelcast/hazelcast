/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.SelfResponseOperation;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isTechnicalCancellationException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologyException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

/**
 * Base class for async operations. Handles registration/deregistration of
 * operations from live registry, exception handling and peeling and
 * logging of exceptions
 */
public abstract class AsyncOperation extends Operation implements SelfResponseOperation, IdentifiedDataSerializable {

    private transient boolean skip;

    @Override
    public void beforeRun() throws Exception {
        JetServiceBackend service = getJetServiceBackend();
        // Due to a bug in the retry invocation mechanism, the same operation with the same callId
        // may be invoked twice. In such cases, we simply ignore the duplicate invocation and avoid
        // sending a response. The response will be handled by the first invocation.
        // See more details:
        // https://hazelcast.atlassian.net/wiki/spaces/EN/pages/5506629766/Inherent+Inconsistencies+in+AP+Subsystem
        skip = !service.getLiveOperationRegistry().register(this);
    }

    @Override
    public final void run() {
        if (skip) {
            getLogger().warning("Skipping operation execution: duplicate operation detected during registration: " + this);
            return;
        }
        CompletableFuture<?> future;
        try {
            future = doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
            return;
        }
        future.whenComplete(withTryCatch(getLogger(), (r, f) -> {
            if (f != null) {
                Throwable peeledFailure = peel(f);
                if (!isTechnicalCancellationException(f)) {
                    logError(peeledFailure);
                }
                doSendResponse(peeledFailure);
            } else {
                doSendResponse(r);
            }
        }));
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
            final JetServiceBackend service = getJetServiceBackend();
            service.getLiveOperationRegistry().deregister(this);
        } finally {
            try {
                sendResponse(value);
            } catch (Exception e) {
                Throwable ex = peel(e);
                if (value instanceof Throwable throwable && ex instanceof HazelcastSerializationException) {
                    // Sometimes exceptions are not serializable, for example on
                    // https://github.com/hazelcast/hazelcast-jet/issues/1995.
                    // When sending exception as a response and the serialization fails,
                    // the response will not be sent and the operation will hang.
                    // To prevent this from happening, replace the exception with
                    // another exception that can be serialized.
                    sendResponse(new JetException(ExceptionUtil.toString(throwable)));
                } else {
                    throw e;
                }
            }
        }
    }

    protected JetServiceBackend getJetServiceBackend() {
        checkJetIsEnabled(getNodeEngine());
        assert getServiceName().equals(JetServiceBackend.SERVICE_NAME) : "Service is not Jet Service";
        return getService();
    }

    protected JobCoordinationService getJobCoordinationService() {
        return getJetServiceBackend().getJobCoordinationService();
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isTopologyException(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public final int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }
}
