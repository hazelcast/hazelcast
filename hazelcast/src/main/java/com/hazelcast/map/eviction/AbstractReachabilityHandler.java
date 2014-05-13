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

package com.hazelcast.map.eviction;

import com.hazelcast.map.record.Record;

import java.io.PrintStream;

/**
 * AbstractReachabilityHandler.
 */
abstract class AbstractReachabilityHandler implements ReachabilityHandler<Record> {

    private static final boolean DEBUG = false;

    protected ReachabilityHandler<Record> successorHandler;

    protected AbstractReachabilityHandler() {
    }

    protected abstract Record handle(Record record, long criteria, long time);

    @Override
    public Record process(Record record, long criteria, long time) {
        final Record handledRecord = handle(record, criteria, time);
        if (handledRecord == null || successorHandler == null) {
            return handledRecord;
        }
        return successorHandler.process(handledRecord, criteria, time);
    }

    @Override
    public void setSuccessorHandler(ReachabilityHandler successorHandler) {
        this.successorHandler = successorHandler;
    }

    @Override
    public ReachabilityHandler<Record> getSuccessorHandler() {
        return successorHandler;
    }

    @Override
    public void resetHandler() {
        successorHandler = null;
    }

    protected static final PrintStream log(String msg, Object... args) {
        if (!DEBUG) {
            return null;
        }
        return System.out.printf(msg + "\n", args);
    }

}
