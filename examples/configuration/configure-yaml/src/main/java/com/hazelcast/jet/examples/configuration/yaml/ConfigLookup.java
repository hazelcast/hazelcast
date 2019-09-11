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
package com.hazelcast.jet.examples.configuration.yaml;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;

public class ConfigLookup {
    public static void main(String[] args) {
        // XML takes precedence: loading jet configuration hazelcast-jet.xml from the classpath
        // despite the presence of hazelcast-jet.yaml
        JetInstance jet = Jet.newJetInstance();
        InstanceConfig instanceConfig = jet.getConfig().getInstanceConfig();
        MetricsConfig metricsConfig = jet.getConfig().getMetricsConfig();
        // the jet instance uses backup count of 3 and metrics enabled as configured in hazelcast-jet.xml
        // the underlying hazelcast member uses default port (5701) from the hazelcast-jet-member-default.xml
        // in the JAR package.
        System.out.println("Backup-count: " + instanceConfig.getBackupCount()
                + ", Metrics enabled:" + metricsConfig.isEnabled()
        );
        jet.shutdown();

        // loading configuration from hazelcast-jet.yaml from the classpath without looking for XML files
        // by using YAML-specific config builder
        JetConfig config = new YamlJetConfigBuilder().build();
        // the jet instance uses backup count of 1 and metrics disabled as configured in hazelcast-jet.yaml
        // the underlying hazelcast member uses the port (7000) from the hazelcast.yaml
        jet = Jet.newJetInstance(config);
        instanceConfig = jet.getConfig().getInstanceConfig();
        metricsConfig = jet.getConfig().getMetricsConfig();
        System.out.println("Backup-count: " + instanceConfig.getBackupCount()
                + ", Metrics enabled:" + metricsConfig.isEnabled()
        );

        // loading the client configuration from hazelcast-client.yaml since no hazelcast-client.xml
        // is present on the classpath
        JetInstance client = Jet.newJetClient();
        // the client is connected to the jet instance running on port 7000

        client.shutdown();
        jet.shutdown();
    }

}
