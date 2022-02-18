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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Sent by master member to other cluster members to publish completed migrations
 * and the new partition state version.
 *
 * @since 3.12
 */
public class PublishCompletedMigrationsOperation extends AbstractPartitionOperation implements MigrationCycleOperation {

    private Collection<MigrationInfo> completedMigrations;

    private transient boolean success;

    public PublishCompletedMigrationsOperation() {
    }

    public PublishCompletedMigrationsOperation(Collection<MigrationInfo> completedMigrations) {
        this.completedMigrations = completedMigrations;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl service = getService();
        success = service.applyCompletedMigrations(completedMigrations, getCallerAddress());
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        completedMigrations = readCollection(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        writeCollection(completedMigrations, out);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PUBLISH_COMPLETED_MIGRATIONS;
    }
}
