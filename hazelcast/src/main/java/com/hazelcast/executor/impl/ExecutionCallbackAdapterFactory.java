/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

class ExecutionCallbackAdapterFactory {

    //Updates the ExecutionCallbackAdapterFactory.done field. An AtomicBoolean is simpler, but creates another unwanted
    //object. Using this approach, you don't create that object.
    private static final AtomicReferenceFieldUpdater<ExecutionCallbackAdapterFactory, Boolean> DONE_FIELD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ExecutionCallbackAdapterFactory.class, Boolean.class, "done");

    private final MultiExecutionCallback multiExecutionCallback;
    private final ConcurrentMap<Member, ValueWrapper> responses;
    private final Collection<Member> members;
    private final ILogger logger;
    @SuppressWarnings("CanBeFinal")
    private volatile Boolean done = Boolean.FALSE;

    ExecutionCallbackAdapterFactory(NodeEngine nodeEngine, Collection<Member> members,
                                    MultiExecutionCallback multiExecutionCallback) {
        this.multiExecutionCallback = multiExecutionCallback;
        this.responses = new ConcurrentHashMap<Member, ValueWrapper>(members.size());
        this.members = new HashSet<Member>(members);
        this.logger = nodeEngine.getLogger(ExecutionCallbackAdapterFactory.class);
    }

    private void onResponse(Member member, Object response) {
        assertNotDone();
        assertIsMember(member);
        placeResponse(member, response);
        triggerOnResponse(member, response);
        triggerOnComplete();
    }

    private void triggerOnComplete() {
        if (members.size() == responses.size() && setDone()) {
            Map<Member, Object> realResponses = new HashMap<Member, Object>(members.size());
            for (Map.Entry<Member, ValueWrapper> entry : responses.entrySet()) {
                Member key = entry.getKey();
                Object value = entry.getValue().value;
                realResponses.put(key, value);
            }
            multiExecutionCallback.onComplete(realResponses);
        }
    }

    private boolean setDone() {
        return DONE_FIELD_UPDATER.compareAndSet(this, Boolean.FALSE, Boolean.TRUE);
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
