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

package com.hazelcast.executor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

class ExecutionCallbackAdapterFactory {

    //Updates the ExecutionCallbackAdapterFactory.done field. An AtomicBoolean is simpler, but creates another unwanted
    //object. Using this approach, you don't create that object.
    private static final AtomicReferenceFieldUpdater<ExecutionCallbackAdapterFactory, Boolean> DONE =
            AtomicReferenceFieldUpdater.newUpdater(ExecutionCallbackAdapterFactory.class, Boolean.class, "done");

    private final MultiExecutionCallback multiExecutionCallback;
    private final ConcurrentMap<Member, ValueWrapper> responses;
    private final Collection<Member> members;
    private final ILogger logger;
    @SuppressWarnings("CanBeFinal")
    private volatile Boolean done = FALSE;

    ExecutionCallbackAdapterFactory(@Nonnull ILogger logger,
                                    @Nonnull Collection<Member> members,
                                    @Nonnull MultiExecutionCallback multiExecutionCallback) {
        checkNotNull(logger, "logger must not be null");
        checkNotNull(members, "members must not be null");
        checkNotNull(multiExecutionCallback, "multiExecutionCallback must not be null");
        this.multiExecutionCallback = multiExecutionCallback;
        this.responses = createConcurrentHashMap(members.size());
        this.members = new HashSet<>(members);
        this.logger = logger;
    }

    private void onResponse(Member member, Object response) {
        assertNotDone();
        assertIsMember(member);
        triggerOnResponse(member, response);
        placeResponse(member, response);
        triggerOnComplete();
    }

    private void triggerOnComplete() {
        if (members.size() != responses.size() || !setDone()) {
            return;
        }

        Map<Member, Object> realResponses = createHashMap(members.size());
        for (Map.Entry<Member, ValueWrapper> entry : responses.entrySet()) {
            Member key = entry.getKey();
            Object value = entry.getValue().value;
            realResponses.put(key, value);
        }
        multiExecutionCallback.onComplete(realResponses);
    }

    private boolean setDone() {
        return DONE.compareAndSet(this, FALSE, TRUE);
    }

    private void triggerOnResponse(Member member, Object response) {
        try {
            multiExecutionCallback.onResponse(member, response);
        } catch (Throwable e) {
            logger.warning(e.getMessage(), e);
        }
    }

    private void placeResponse(Member member, Object response) {
        ValueWrapper current = responses.put(member, new ValueWrapper(response));

        if (current != null) {
            logger.warning("Replacing current callback value[" + current.value
                    + " with value[" + response + "].");
        }
    }

    private void assertIsMember(Member member) {
        if (!members.contains(member)) {
            throw new IllegalArgumentException(member + " is not known by this callback!");
        }
    }

    private void assertNotDone() {
        if (done) {
            throw new IllegalStateException("This callback is invalid!");
        }
    }

    <V> ExecutionCallback<V> callbackFor(Member member) {
        return new InnerExecutionCallback<V>(member);
    }

    private static final class ValueWrapper {
        final Object value;

        private ValueWrapper(Object value) {
            this.value = value;
        }
    }

    private final class InnerExecutionCallback<V> implements ExecutionCallback<V> {
        private final Member member;

        private InnerExecutionCallback(Member member) {
            this.member = member;
        }

        @Override
        public void onResponse(V response) {
            ExecutionCallbackAdapterFactory.this.onResponse(member, response);
        }

        @Override
        public void onFailure(Throwable t) {
            ExecutionCallbackAdapterFactory.this.onResponse(member, t);
        }
    }
}
