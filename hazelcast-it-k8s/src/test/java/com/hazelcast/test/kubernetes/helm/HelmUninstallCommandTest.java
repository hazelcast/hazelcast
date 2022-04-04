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

import static org.assertj.core.api.Assertions.assertThat;

public class HelmUninstallCommandTest {
    private final DummyCommandRunner dummyCommandRunner = new DummyCommandRunner();

    @Test
    public void should_uninstall_with_default_namespace() {
        HelmUninstallCommand helmUninstallCommand = new HelmUninstallCommand("some-release-name", "some-namespace", dummyCommandRunner);

        helmUninstallCommand.run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm uninstall --namespace some-namespace some-release-name");
    }

    @Test
    public void should_uninstall_with_no_namespace() {
        HelmUninstallCommand helmUninstallCommand = new HelmUninstallCommand("some-release-name", null, dummyCommandRunner);

        helmUninstallCommand.run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm uninstall some-release-name");
    }

    @Test
    public void should_uninstall_from_custom_namespace() {
        HelmUninstallCommand helmUninstallCommand = new HelmUninstallCommand("some-release-name", "some-namespace", dummyCommandRunner);

        helmUninstallCommand.withNamespace("custom-namespace").run();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm uninstall --namespace custom-namespace some-release-name");
    }

}
