/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
    protected static final String MC_VERSION_NAME = "mc.version";

    private final String toolVersion;
    private final String hzVersion;
    private final String mcVersion;

    VersionProvider()
            throws IOException {
        Properties commandlineProperties = new Properties();
        commandlineProperties.load(getClass().getClassLoader().getResourceAsStream("version.properties"));
        toolVersion = commandlineProperties.getProperty(TOOL_VERSION_NAME);
        hzVersion = commandlineProperties.getProperty(HZ_VERSION_NAME);
        mcVersion = commandlineProperties.getProperty(MC_VERSION_NAME);
    }

    VersionProvider(String toolVersion, String hzVersion, String mcVersion) {
        this.toolVersion = toolVersion;
        this.hzVersion = hzVersion;
        this.mcVersion = mcVersion;
    }

    public String[] getVersion() {
        return new String[]{"CLI tool: " + toolVersion,
                "Hazelcast: " + hzVersion,
                "Hazelcast Management Center: " + mcVersion};
    }

    String getMcVersion() {
        return mcVersion;
    }
}
