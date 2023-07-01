/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.JetJoinInfo;

public class JdbcJoiner {

    NestedLoopReaderParams nestedLoopReaderParams;

    public JdbcJoiner(NestedLoopReaderParams nestedLoopReaderParams) {
        this.nestedLoopReaderParams = nestedLoopReaderParams;
    }

    public ProcessorSupplier createProcessorSupplier() {
        ProcessorSupplier processorSupplier = null;
        JetJoinInfo joinInfo = nestedLoopReaderParams.getJoinInfo();
        if (joinInfo.isInner()) {
            if (!joinInfo.isEquiJoin()) {
                // Indices are not given
                processorSupplier = createInnerJoinFullScanProcessorSupplier();
            } else if (joinInfo.isEquiJoin()) {
                // Indices are given
                processorSupplier = createInnerJoinIndexScanProcessorSupplier();
            }
        }
        return processorSupplier;
    }

    ProcessorSupplier createInnerJoinFullScanProcessorSupplier() {
        SelectQueryBuilder builder = new SelectQueryBuilder(
                nestedLoopReaderParams.getJdbcTable(),
                nestedLoopReaderParams.getSqlDialect(),
                nestedLoopReaderParams.getRexPredicate(),
                nestedLoopReaderParams.getRexProjection()
        );
        String query = builder.query();
        return new JdbcJoinScanProcessorSupplier(nestedLoopReaderParams, query);
    }

    ProcessorSupplier createInnerJoinIndexScanProcessorSupplier() {
        return null;
    }
}
