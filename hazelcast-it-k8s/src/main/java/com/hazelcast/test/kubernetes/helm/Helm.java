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

public class Helm {
    private final Repo repo;
    private final CommandRunner commandRunner;
    private final String namespace;

    public Helm() {
        this(null);
    }

    public Helm(String defaultNamespace) {
        this(defaultNamespace, CommandRunner.getDefault());
    }

    Helm(String defaultNamespace, CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
        this.repo = new Repo(commandRunner);
        this.namespace = defaultNamespace;
    }

    public Repo repo() {
        return repo;
    }

    public HelmInstallCommand installCommand(String releaseName, String chartName) {
        return new HelmInstallCommand(releaseName, chartName, namespace, commandRunner);
    }

    public HelmUpgradeCommand upgradeCommand(String releaseName, String chartName) {
        return new HelmUpgradeCommand(releaseName, chartName, namespace, commandRunner);
    }

    public HelmUninstallCommand uninstallCommand(String releaseName) {
        return new HelmUninstallCommand(releaseName, namespace, commandRunner);
    }
}
