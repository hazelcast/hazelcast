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

import java.util.Properties;

public class YamlJetConfigClasspath {
    public static void main(String[] args) throws Exception {
        // taking the metrics and backup configuration from system properties.
        System.setProperty("metricsEnabled", "true");
        System.setProperty("backupCount", "4");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        JetConfig config = JetConfig.loadFromClasspath(classLoader, "hazelcast-jet-sample.yaml");
        JetInstance jet = Jet.newJetInstance(config);
        printInstanceAndMetricsConfig(jet);
        jet.shutdown();

        // taking the metrics and backup configuration from provided properties instance
        Properties configProperties = new Properties();
        configProperties.setProperty("metricsEnabled", "true");
        configProperties.setProperty("backupCount", "4");
        config = JetConfig.loadFromClasspath(classLoader, "hazelcast-jet-sample.yaml", configProperties);
        jet = Jet.newJetInstance(config);
        printInstanceAndMetricsConfig(jet);
        jet.shutdown();
    }

    private static void printInstanceAndMetricsConfig(JetInstance jet) {
        InstanceConfig instanceConfig = jet.getConfig().getInstanceConfig();
        MetricsConfig metricsConfig = jet.getConfig().getMetricsConfig();
        System.out.println("Backup-count: " + instanceConfig.getBackupCount()
                + ", Metrics enabled:" + metricsConfig.isEnabled()
        );
    }
}
