/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.sql.impl.row.JetSqlJoinRow;

import javax.annotation.Nonnull;
import java.util.Arrays;

class MergerProcessor extends AbstractProcessor {
    private JetSqlJoinRow currentResult;

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        if (getLogger().isFinestEnabled()) {
            getLogger().info("merger local index " + context.localProcessorIndex()
                    + " global index " + context.globalProcessorIndex()
                    + ", processorPartitions=" + Arrays.toString(context.processorPartitions()));
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        JetSqlJoinRow row = (JetSqlJoinRow) item;

        assert currentResult == null || currentResult.getRowId() <= row.getRowId()
                : "Disorder during merge: current " + currentResult + " > new " + row;

        getLogger().info("merge " + row + " current: " + currentResult);

        // TODO: emit when we got all results (count them) without waiting for next record/complete
        //  so in streaming case we do not delay the record
        if (currentResult == null || currentResult.getRowId() < row.getRowId()) {
            // we got new row - emit current if needed
            if (!tryEmitCurrentResult()) {
                return false;
            }

            return onNew(row);
        } else {
            // next result for the same row
            return onNext(row);
        }
    }

    private boolean onNew(JetSqlJoinRow row) {
        // emit before saving. After save, the row would not be "new"
        if (row.isMatched() && !tryEmit(row)) {
            return false;
        }
        currentResult = row;
        return true;
    }

    private boolean onNext(JetSqlJoinRow row) {
        if (!row.isMatched()) {
            // ignore. either current row is matched (so not matched is irrelevant but we cannot overwrite currentResult)
            // or current is not matched - the same as new one, so does not matter which we use.
            return true;
        }

        if (!tryEmit(row)) {
            return false;
        }
        // current result might have been unmatched so update
        currentResult = row;
        return true;
    }


    private boolean tryEmitCurrentResult() {
        if (currentResult == null) {
            return true;
        }

        // TODO: this is for outer join. support other modes (anti/semi)
        // return unmatched row if there was no match
        if (!currentResult.isMatched() && !tryEmit(currentResult)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean complete() {
        getLogger().info("complete " + currentResult);

        return tryEmitCurrentResult();
    }
}
