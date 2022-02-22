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

package com.hazelcast.executor.impl.operations;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.executor.impl.ExecutorDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import javax.annotation.Nonnull;
import java.util.UUID;

public final class MemberCallableTaskOperation extends AbstractCallableTaskOperation
        implements IdentifiedDataSerializable, MutatingOperation {

    public MemberCallableTaskOperation() {
    }

    public MemberCallableTaskOperation(String name,
                                       UUID uuid,
                                       @Nonnull Data callableData) {
        super(name, uuid, callableData);
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public int getClassId() {
        return ExecutorDataSerializerHook.MEMBER_CALLABLE_TASK;
    }
}
