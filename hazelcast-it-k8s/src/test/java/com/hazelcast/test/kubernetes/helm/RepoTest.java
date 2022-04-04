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

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

public class RepoTest {
    private final DummyCommandRunner dummyCommandRunner = new DummyCommandRunner();

    @Test
    public void should_add_repo() throws Exception {
        Repo repo = new Repo(dummyCommandRunner);

        repo.add("some-name", new URL("https://example.org"));

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm repo add some-name https://example.org");
    }

    @Test
    public void should_update() throws Exception {
        Repo repo = new Repo(dummyCommandRunner);

        repo.update();

        assertThat(dummyCommandRunner.getLastCommand()).isEqualTo("helm repo update");
    }

}
