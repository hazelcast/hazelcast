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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;

public class JobUpdate {
    public static void main(String[] args) {
        //tag::s1[]
        JetInstance client = Jet.newJetClient();
        Job job = client.getJob("foo-job");
        // This call will block until the snapshot is written
        job.cancelAndExportSnapshot("foo-state-name");

        // Code for the new pipeline
        Pipeline p = Pipeline.create();
        JobConfig config = new JobConfig()
                .setInitialSnapshotName("foo-state-name");
        client.newJob(p, config);

        // Now make sure the new job is running and restored from the
        // state correctly. When you no longer need the snapshot, delete
        // it. Jet never deletes exported snapshots automatically.
        client.getJobStateSnapshot("foo-state-name").destroy();
        //end::s1[]
    }
}
