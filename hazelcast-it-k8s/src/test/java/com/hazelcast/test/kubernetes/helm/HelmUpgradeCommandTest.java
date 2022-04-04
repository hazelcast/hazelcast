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

public class HelmUpgradeCommandTest {
    private final DummyCommandRunner dummyCommandRunner = new DummyCommandRunner();

    @Test
    public void should_upgrade() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                "some-namespace", dummyCommandRunner);

        helmInstall.run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name --namespace some-namespace");
    }

    @Test
    public void should_upgrade_with_values() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                "some-namespace", dummyCommandRunner);

        helmInstall.addValue("some-key", "some-value").run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name --namespace some-namespace --set some-key=some-value");
    }

    @Test
    public void should_upgrade_with_multiple_values() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                "some-namespace", dummyCommandRunner);

        helmInstall.addValue("some-key", "some-value").addValue("some-other-key", "some-other-value").run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name --namespace some-namespace --set some-other-key=some-other-value --set some-key=some-value");
    }

    @Test
    public void should_upgrade_with_custom_values_file() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                "some-namespace", dummyCommandRunner);

        helmInstall.addValuesFile(Paths.get("some-values-file.yml")).run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name --namespace some-namespace --values some-values-file.yml");
    }

    @Test
    public void should_upgrade_with_multiple_values_file() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                null, dummyCommandRunner);

        helmInstall.addValuesFile(Paths.get("some-values-file.yml")).addValuesFile(Paths.get("some-other-values-file.yml")).run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name --values some-values-file.yml --values some-other-values-file.yml");
    }

    @Test
    public void should_upgrade_with_no_namespace() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                null, dummyCommandRunner);

        helmInstall.run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name");
    }

    @Test
    public void should_upgrade_with_install() {
        HelmUpgradeCommand helmInstall = new HelmUpgradeCommand("some-release-name", "some-chart-name",
                "some-namespace", dummyCommandRunner);

        helmInstall.withInstall().run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm upgrade some-release-name some-chart-name --namespace some-namespace --install");
    }
}
