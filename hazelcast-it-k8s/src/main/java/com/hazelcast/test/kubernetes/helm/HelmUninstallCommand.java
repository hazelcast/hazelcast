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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HelmUninstallCommand {
    private final String releaseName;
    private String namespace;
    private final CommandRunner commandRunner;

    HelmUninstallCommand(String releaseName, String defaultNamespace, CommandRunner commandRunner) {
        this.releaseName = releaseName;
        this.namespace = defaultNamespace;
        this.commandRunner = commandRunner;
    }

    public HelmUninstallCommand withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public void run() {
        List<String> command = new ArrayList<>(Arrays.asList("helm", "uninstall"));
        if (namespace != null) {
            command.add("--namespace");
            command.add(namespace);
        }
        command.add(releaseName);
        commandRunner.run(command);
    }
}
