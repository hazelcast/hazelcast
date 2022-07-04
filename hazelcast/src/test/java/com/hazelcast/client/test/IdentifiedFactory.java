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

package com.hazelcast.client.test;

import com.hazelcast.client.test.executor.tasks.AppendCallable;
import com.hazelcast.client.test.executor.tasks.CancellationAwareTask;
import com.hazelcast.client.test.executor.tasks.FailingCallable;
import com.hazelcast.client.test.executor.tasks.GetMemberUuidTask;
import com.hazelcast.client.test.executor.tasks.MapPutPartitionAwareCallable;
import com.hazelcast.client.test.executor.tasks.NullCallable;
import com.hazelcast.client.test.executor.tasks.SelectAllMembers;
import com.hazelcast.client.test.executor.tasks.SelectNoMembers;
import com.hazelcast.client.test.executor.tasks.SerializedCounterCallable;
import com.hazelcast.client.test.executor.tasks.TaskWithUnserializableResponse;
import com.hazelcast.client.test.ifunction.AppendString;
import com.hazelcast.client.test.ifunction.Multiplication;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class IdentifiedFactory implements DataSerializableFactory {
    public static final int FACTORY_ID = 66;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == IdentifiedEntryProcessor.CLASS_ID) {
            return new IdentifiedEntryProcessor();
        }
        if (typeId == CustomComparator.CLASS_ID) {
            return new CustomComparator();
        }
        if (typeId == DistortInvalidationMetadataEntryProcessor.CLASS_ID) {
            return new DistortInvalidationMetadataEntryProcessor();
        }
        if (typeId == PrefixFilter.CLASS_ID) {
            return new PrefixFilter();
        }
        if (typeId == AppendCallable.CLASS_ID) {
            return new AppendCallable();
        }
        if (typeId == CancellationAwareTask.CLASS_ID) {
            return new CancellationAwareTask();
        }
        if (typeId == FailingCallable.CLASS_ID) {
            return new FailingCallable();
        }
        if (typeId == GetMemberUuidTask.CLASS_ID) {
            return new GetMemberUuidTask();
        }
        if (typeId == MapPutPartitionAwareCallable.CLASS_ID) {
            return new MapPutPartitionAwareCallable();
        }
        if (typeId == NullCallable.CLASS_ID) {
            return new NullCallable();
        }
        if (typeId == SelectAllMembers.CLASS_ID) {
            return new SelectAllMembers();
        }
        if (typeId == SelectNoMembers.CLASS_ID) {
            return new SelectNoMembers();
        }
        if (typeId == SerializedCounterCallable.CLASS_ID) {
            return new SerializedCounterCallable();
        }
        if (typeId == TaskWithUnserializableResponse.CLASS_ID) {
            return new TaskWithUnserializableResponse();
        }
        if (typeId == CustomCredentials.CLASS_ID) {
            return new CustomCredentials();
        }
        if (typeId == Multiplication.CLASS_ID) {
            return new Multiplication();
        }
        if (typeId == AppendString.CLASS_ID) {
            return new AppendString();
        }
        return null;
    }
}
