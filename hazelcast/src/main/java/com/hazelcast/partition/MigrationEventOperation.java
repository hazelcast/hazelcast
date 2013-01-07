///*
// * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.partition;
//
//import com.hazelcast.spi.AbstractOperation;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
//public class MigrationEventOperation extends AbstractOperation {
//
//    private MigrationInfo migrationInfo;
//    private MigrationStatus status;
//
//    public MigrationEventOperation() {
//    }
//
//    public MigrationEventOperation(final MigrationStatus status, final MigrationInfo migrationInfo) {
//        this.status = status;
//        this.migrationInfo = migrationInfo;
//    }
//
//    public void run() {
//        PartitionService partitionService = getService();
//        partitionService.fireMigrationEvent(migrationInfo, status);
//    }
//
//    @Override
//    public void readInternal(ObjectDataInput in) throws IOException {
//        super.readInternal(in);
//        migrationInfo = new MigrationInfo();
//        migrationInfo.readData(in);
//        status = MigrationStatus.readFrom(in);
//    }
//
//    @Override
//    public void writeInternal(ObjectDataOutput out) throws IOException {
//        super.writeInternal(out);
//        migrationInfo.writeData(out);
//        MigrationStatus.writeTo(status, out);
//    }
//
//}