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

package com.hazelcast.jet.impl;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataKey;
import com.hazelcast.map.IMap;

import static com.hazelcast.jet.impl.JobExecutionRecord.NO_SNAPSHOT;

final class SnapshotValidator {

    private SnapshotValidator() {
    }

    /**
     * Validates a snapshot with the given id.
     *
     * @param snapshotId snapshot ID or {@link JobExecutionRecord#NO_SNAPSHOT}
     *                   if snapshot ID is not known
     * @param snapshotMap snapshot map to validate
     * @param jobIdString name and ID of the job, for debug output
     * @param snapshotName user-supplied snapshot name for debug output,
     *                     null if it's not an exported snapshot
     * @return the snapshot ID of the snapshot being validated
     */
    static long validateSnapshot(
            long snapshotId, IMap<Object, Object> snapshotMap, String jobIdString, String snapshotName
    ) {
        SnapshotValidationRecord validationRecord =
                (SnapshotValidationRecord) snapshotMap.get(SnapshotValidationRecord.KEY);
        if (validationRecord == null) {
            String nameOrId = snapshotName != null ? '"' + snapshotName + '"' : "with ID " + snapshotId;
            throw new JetException(String.format(
                    "snapshot %s in IMap %s (%d entries) is damaged. Unable to restore the state for %s.",
                    nameOrId, snapshotMap.getName(), snapshotMap.size(), jobIdString));
        }
        if (validationRecord.numChunks() != snapshotMap.size() - 1) {
            // fallback validation that counts using aggregate(), ignoring different snapshot IDs
            long filteredCount = snapshotMap.aggregate(
                    Aggregators.count(),
                    e -> e.getKey() instanceof SnapshotDataKey
                            && ((SnapshotDataKey) e.getKey()).snapshotId() == snapshotId);
            if (validationRecord.numChunks() != filteredCount) {
                throw new JetException(String.format(
                        "State for %s in IMap '%s' is corrupted: it should have %,d entries, but has %,d",
                        jobIdString, snapshotMap.getName(), validationRecord.numChunks(), snapshotMap.size() - 1));
            }
        }
        if (snapshotId != NO_SNAPSHOT && snapshotId != validationRecord.snapshotId()) {
            throw new JetException(String.format(
                    "%s: IMap '%s' was supposed to contain snapshotId %d, but it contains snapshotId %d",
                    jobIdString, snapshotMap.getName(), snapshotId, validationRecord.snapshotId()));
        }
        return validationRecord.snapshotId();
    }
}
