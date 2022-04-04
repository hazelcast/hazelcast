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
package com.hazelcast.test.kubernetes.helm;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HelmInstallCommand {
    private final String releaseName;
    private final String chartName;
    private String namespace;
    private final Map<String, String> values = new HashMap<>();
    private final CommandRunner commandRunner;
    private final List<Path> valuesFiles = new ArrayList<>();

    HelmInstallCommand(String releaseName, String chartName, String defaultNamespace, CommandRunner commandRunner) {
        this.releaseName = releaseName;
        this.chartName = chartName;
        this.namespace = defaultNamespace;
        this.commandRunner = commandRunner;
    }

    public HelmInstallCommand addValuesFile(Path valuesFile) {
        valuesFiles.add(valuesFile);
        return this;
    }

    public HelmInstallCommand addValue(String name, Object value) {
        values.put(name, value.toString());
        return this;
    }

    public HelmInstallCommand withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public void run() {
        List<String> command = new ArrayList<>(Arrays.asList("helm", "install", releaseName, chartName));
        if (namespace != null) {
            command.add("--namespace");
            command.add(namespace);
        }
        for (Path valuesFile : valuesFiles) {
            command.add("--values");
            command.add(valuesFile.toString());
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            command.add("--set");
            command.add(entry.getKey() + "=" + entry.getValue());
        }
        commandRunner.run(command);
    }
}
