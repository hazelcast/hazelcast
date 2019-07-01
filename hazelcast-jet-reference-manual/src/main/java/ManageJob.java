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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.pipeline.Pipeline;

import java.util.List;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ManageJob {
    private static JetInstance jet = Jet.newJetInstance();

    static void s1() {
        //tag::s1[]
        Pipeline pipeline = buildPipeline();
        jet.newJob(pipeline).join();
        //end::s1[]

        //tag::s2[]
        DAG dag = buildDag();
        jet.newJob(dag).join();
        //end::s2[]

        //tag::s3[]
        JobConfig cfg = new JobConfig();
        cfg.setName("my-job");
        jet.newJob(pipeline, cfg);
        //end::s3[]
    }

    static DAG buildDag() {
        return new DAG();
    }

    static Pipeline buildPipeline() {
        return Pipeline.create();
    }

    static void s4() {
        //tag::s4[]
        int total = 0;
        int completed = 0;
        int failed = 0;
        int inProgress = 0;
        long fiveMinutesAgo = System.currentTimeMillis() - MINUTES.toMillis(5);
        for (Job job : jet.getJobs()) {
            if (job.getSubmissionTime() < fiveMinutesAgo) {
                continue;
            }
            total++;
            switch (job.getStatus()) {
                case COMPLETED:
                    completed++;
                    break;
                case FAILED:
                    failed++;
                    break;
                default:
                    inProgress++;
            }
        }
        System.out.format(
            "In the last five minutes %d jobs were submitted to Jet, of "
            + "which %d already completed, %d jobs failed, and %d jobs "
            + "are still running.%n",
            total, completed, failed, inProgress);
        //end::s4[]
    }

    static void s5() {
        //tag::s5[]
        List<Job> myJobs = jet.getJobs("my-job");
        long failedCount = myJobs.stream().filter(j -> j.getStatus() == FAILED).count();
        System.out.format("Jet ran 'my-job' %d times and it failed %d times.%n",
                myJobs.size(), failedCount);
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        Pipeline pipeline = buildPipeline();
        JobConfig cfg = new JobConfig();
        cfg.setName("my-job");
        Job job1 = jet.newJobIfAbsent(pipeline, cfg);
        Job job2 = jet.newJobIfAbsent(pipeline, cfg);

        assert job1.getId() == job2.getId();
        //end::s6[]
    }

    static void s7() {
        Job job = null;
        //tag::s7[]
        job.cancel();
        //end::s7[]
    }

    static void s8() {
        Job job = null;
        //tag::s8[]
        job.cancelAndExportSnapshot("foo-snapshot");
        //end::s8[]
    }

    static void s9() {
        Job job = null;
        //tag::s9[]
        job.exportSnapshot("foo-snapshot");
        //end::s9[]
    }

    static void s10() {
        JetInstance instance = null;
        //tag::s10[]
        Pipeline updatedPipeline = Pipeline.create();
        // create the pipeline...

        JobConfig jobConfig = new JobConfig();
        jobConfig.setInitialSnapshotName("foo-snapshot");
        instance.newJob(updatedPipeline, jobConfig);
        //end::s10[]
    }
}
