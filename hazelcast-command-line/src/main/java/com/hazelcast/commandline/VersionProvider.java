/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.commandline;

import picocli.CommandLine;

import java.io.IOException;
import java.util.Properties;

/**
 * Implementation of {@link picocli.CommandLine.IVersionProvider} for providing version information.
 */
class VersionProvider
        implements CommandLine.IVersionProvider {
    protected static final String TOOL_VERSION_NAME = "tool.version";
    protected static final String HZ_VERSION_NAME = "hz.version";

    private final String toolVersion;
    private final String hzVersion;

    VersionProvider()
            throws IOException {
        Properties commandlineProperties = new Properties();
        commandlineProperties.load(getClass().getClassLoader().getResourceAsStream("version.properties"));
        toolVersion = commandlineProperties.getProperty(TOOL_VERSION_NAME);
        hzVersion = commandlineProperties.getProperty(HZ_VERSION_NAME);
    }

    VersionProvider(String toolVersion, String hzVersion) {
        this.toolVersion = toolVersion;
        this.hzVersion = hzVersion;
    }

    public String[] getVersion() {
        return new String[]{"CLI tool: " + toolVersion,
                "Hazelcast: " + hzVersion};
    }

}
