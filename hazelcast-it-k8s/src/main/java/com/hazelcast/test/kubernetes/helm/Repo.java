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

import java.net.URL;

public class Repo {
    private final CommandRunner commandRunner;

    Repo(CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
    }

    public void add(String name, URL url) {
        commandRunner.run("helm", "repo", "add", name, url.toString());
    }

    public void update() {
        commandRunner.run("helm", "repo", "update");
    }
}
