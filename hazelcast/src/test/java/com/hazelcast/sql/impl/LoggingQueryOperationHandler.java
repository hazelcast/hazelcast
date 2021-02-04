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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

public class LoggingQueryOperationHandler implements QueryOperationHandler {

    private final LinkedBlockingQueue<SubmitInfo> submitInfos = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ExecuteInfo> executeInfos = new LinkedBlockingQueue<>();

    @Override
    public boolean submit(UUID sourceMemberId, UUID memberId, QueryOperation operation) {
        submitInfos.add(new SubmitInfo(sourceMemberId, memberId, operation));

        return true;
    }

    @Override
    public void execute(QueryOperation operation) {
        executeInfos.add(new ExecuteInfo(operation, Thread.currentThread().getName()));
    }

    public SubmitInfo tryPollSubmitInfo() {
        List<SubmitInfo> infos = tryPollSubmitInfos(1);

        return infos != null ? infos.get(0) : null;
    }

    public List<SubmitInfo> tryPollSubmitInfos(int count) {
        if (submitInfos.size() >= count) {
            List<SubmitInfo> res = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                res.add(submitInfos.poll());
            }

            return res;
        } else {
            return null;
        }
    }

    public ExecuteInfo tryPollExecuteInfo() {
        List<ExecuteInfo> infos = tryPollExecuteInfos(1);

        return infos != null ? infos.get(0) : null;
    }

    public List<ExecuteInfo> tryPollExecuteInfos(int count) {
        if (executeInfos.size() >= count) {
            List<ExecuteInfo> res = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                res.add(executeInfos.poll());
            }

            return res;
        } else {
            return null;
        }
    }

    public static class SubmitInfo {

        private final UUID sourceMemberId;
        private final UUID memberId;
        private final QueryOperation operation;

        private SubmitInfo(UUID sourceMemberId, UUID memberId, QueryOperation operation) {
            this.sourceMemberId = sourceMemberId;
            this.memberId = memberId;
            this.operation = operation;
        }

        public UUID getSourceMemberId() {
            return sourceMemberId;
        }

        public UUID getMemberId() {
            return memberId;
        }

        @SuppressWarnings("unchecked")
        public <T extends QueryOperation> T getOperation() {
            return (T) operation;
        }
    }

    public static class ExecuteInfo {

        private final QueryOperation operation;
        private final String threadName;

        private ExecuteInfo(QueryOperation operation, String threadName) {
            this.operation = operation;
            this.threadName = threadName;
        }

        public QueryOperation getOperation() {
            return operation;
        }

        public String getThreadName() {
            return threadName;
        }
    }
}
