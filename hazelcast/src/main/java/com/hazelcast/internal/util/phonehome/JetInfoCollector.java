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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetServiceBackend;

import java.util.function.BiConsumer;

public class JetInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        JetConfig jetConfig = node.getNodeEngine().getConfig().getJetConfig();
        boolean isJetEnabled = jetConfig.isEnabled();
        metricsConsumer.accept(PhoneHomeMetrics.JET_ENABLED, String.valueOf(isJetEnabled));
        metricsConsumer.accept(PhoneHomeMetrics.JET_RESOURCE_UPLOAD_ENABLED, String.valueOf(jetConfig.isResourceUploadEnabled()));
        if (isJetEnabled) {
            JetServiceBackend jetServiceBackend = node.getNodeEngine().getService(JetServiceBackend.SERVICE_NAME);
            long jobSubmittedCount = jetServiceBackend.getJobCoordinationService().getJobSubmittedCount();
            metricsConsumer.accept(PhoneHomeMetrics.JET_JOBS_SUBMITTED, String.valueOf(jobSubmittedCount));
        }
    }
}
