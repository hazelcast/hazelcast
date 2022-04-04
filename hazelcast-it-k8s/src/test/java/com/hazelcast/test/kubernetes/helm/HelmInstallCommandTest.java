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

import org.junit.Test;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class HelmInstallCommandTest {
    private final DummyCommandRunner dummyCommandRunner = new DummyCommandRunner();

    @Test
    public void should_install() {
        HelmInstallCommand helmInstallCommand = new HelmInstallCommand("some-release-name", "some-chart-name", "some-namespace", dummyCommandRunner);

        helmInstallCommand.run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm install some-release-name some-chart-name --namespace some-namespace");
    }

    @Test
    public void should_install_with_custom_values() {
        HelmInstallCommand helmInstallCommand = new HelmInstallCommand("some-release-name", "some-chart-name", "some-namespace", dummyCommandRunner);

        helmInstallCommand.addValue("some-key", "some-value").run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm install some-release-name some-chart-name --namespace some-namespace --set some-key=some-value");
    }

    @Test
    public void should_install_with_custom_values_file() {
        HelmInstallCommand helmInstallCommand = new HelmInstallCommand("some-release-name", "some-chart-name", "some-namespace", dummyCommandRunner);

        helmInstallCommand.addValuesFile(Paths.get("some-values-file.yml")).run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm install some-release-name some-chart-name --namespace some-namespace --values some-values-file.yml");
    }

    @Test
    public void should_install_with_no_namespace() {
        HelmInstallCommand helmInstallCommand = new HelmInstallCommand("some-release-name", "some-chart-name", null, dummyCommandRunner);

        helmInstallCommand.run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm install some-release-name some-chart-name");
    }
}
