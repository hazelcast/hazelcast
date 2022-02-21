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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.MigrationState;

import java.io.IOException;
import java.util.Objects;

/**
 * Summary of the migration state.
 * Main implementation of {@link MigrationState}.
 */
public class MigrationStateImpl implements MigrationState, IdentifiedDataSerializable {

    private long startTime;
    private int plannedMigrations;
    private int completedMigrations;
    private long totalElapsedTime;

    public MigrationStateImpl() {
    }

    public MigrationStateImpl(long startTime, int plannedMigrations, int completedMigrations, long totalElapsedTime) {
        this.startTime = startTime;
        this.plannedMigrations = plannedMigrations;
        this.completedMigrations = completedMigrations;
        this.totalElapsedTime = totalElapsedTime;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public int getPlannedMigrations() {
        return plannedMigrations;
    }

    @Override
    public int getCompletedMigrations() {
        return completedMigrations;
    }

    @Override
    public int getRemainingMigrations() {
        return plannedMigrations - completedMigrations;
    }

    @Override
    public long getTotalElapsedTime() {
        return totalElapsedTime;
    }

    public MigrationStateImpl onComplete(long elapsed) {
        return onComplete(1, elapsed);
    }

    public MigrationStateImpl onComplete(int migrations, long elapsed) {
        return new MigrationStateImpl(startTime, plannedMigrations, completedMigrations + migrations, totalElapsedTime + elapsed);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(startTime);
        out.writeLong(totalElapsedTime);
        out.writeInt(plannedMigrations);
        out.writeInt(completedMigrations);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        startTime = in.readLong();
        totalElapsedTime = in.readLong();
        plannedMigrations = in.readInt();
        completedMigrations = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.MIGRATION_EVENT;
    }

    @Override
    public String toString() {
        return "MigrationState{startTime=" + startTime + ", plannedMigrations=" + plannedMigrations
                + ", completedMigrations=" + completedMigrations + ", totalElapsedTime=" + totalElapsedTime + "ms}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MigrationStateImpl)) {
            return false;
        }
        MigrationStateImpl that = (MigrationStateImpl) o;
        return startTime == that.startTime
                && plannedMigrations == that.plannedMigrations
                && completedMigrations == that.completedMigrations
                && totalElapsedTime == that.totalElapsedTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, plannedMigrations, completedMigrations, totalElapsedTime);
    }
}
