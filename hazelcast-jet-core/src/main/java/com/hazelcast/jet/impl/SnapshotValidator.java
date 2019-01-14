/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataKey;

import static com.hazelcast.jet.impl.JobExecutionRecord.NO_SNAPSHOT;

final class SnapshotValidator {

    private SnapshotValidator() {
    }

    /**
     * Validates a snapshot with the given id.
     *
     * @param snapshotId snapshot ID or {@link JobExecutionRecord#NO_SNAPSHOT},
     *      if snapshot id is not known
     * @param jobIdString name and id of the job (for debug output)
     * @param map snapshot map to validate
     * @return the snapshot id of the snapshot being validated
     */
    static long validateSnapshot(long snapshotId, String jobIdString, IMap<Object, Object> map) {
        SnapshotValidationRecord validationRecord = (SnapshotValidationRecord) map.get(SnapshotValidationRecord.KEY);
        if (validationRecord == null) {
            throw new JetException("State for " + jobIdString + " was supposed to be restored from '" + map.getName()
                    + "', but that map doesn't contain the validation key: not an IMap with Jet snapshot or corrupted");
        }
        if (validationRecord.numChunks() != map.size() - 1) {
            // fallback validation that counts using aggregate(), ignoring different snapshot IDs
            Long filteredCount = map.aggregate(Aggregators.count(), e -> e.getKey() instanceof SnapshotDataKey
                    && ((SnapshotDataKey) e.getKey()).snapshotId() == snapshotId);
            if (validationRecord.numChunks() != filteredCount) {
                throw new JetException("State for " + jobIdString + " in '" + map.getName() + "' corrupted: it should " +
                        "have " + validationRecord.numChunks() + " entries, but has " + (map.size() - 1) + " entries");
            }
        }
        if (snapshotId != NO_SNAPSHOT && snapshotId != validationRecord.snapshotId()) {
            throw new JetException(jobIdString + ": '" + map.getName() + "' was supposed to contain snapshotId="
                    + snapshotId + ", but it contains snapshotId=" + validationRecord.snapshotId());
        }
        return validationRecord.snapshotId();
    }
}
