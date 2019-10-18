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

package integration;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MeasurementPredicates;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.pipeline.Pipeline;

import java.util.Collection;
import java.util.function.Predicate;

public class Metrics {

    static void s1() {
        //tag::s1[]
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().getMetricsConfig()
                 .setEnabled(true)
                 .setJmxEnabled(true)
                .setRetentionSeconds(5)
                .setCollectionIntervalSeconds(5)
                .setMetricsForDataStructuresEnabled(false);
        //end::s1[]
    }

    static void s2() {
        JetInstance jet = Jet.newJetInstance();
        Pipeline p = Pipeline.create();
        Job job = jet.newJob(p);
        JobMetrics jobMetrics = job.getMetrics();
        //tag::s2[]
        Predicate<Measurement> vertexOfInterest =
                MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, "vertexA");
        Predicate<Measurement> notSnapshotEdge =
                MeasurementPredicates.tagValueEquals(MetricTags.ORDINAL, "snapshot").negate();

        Collection<Measurement> measurements = jobMetrics
                .filter(vertexOfInterest.and(notSnapshotEdge))
                .get(MetricNames.EMITTED_COUNT);

        long totalCount = measurements.stream().mapToLong(Measurement::getValue).sum();
        //end::s2[]
    }
}
