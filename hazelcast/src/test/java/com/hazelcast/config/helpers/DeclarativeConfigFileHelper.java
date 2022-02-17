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

package com.hazelcast.config.helpers;

import com.hazelcast.config.Config;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class DeclarativeConfigFileHelper {
    private static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    private static final String HAZELCAST_END_TAG = "</hazelcast>\n";
    private static final String HAZELCAST_CLIENT_START_TAG =
            "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";
    private static final String HAZELCAST_CLIENT_END_TAG = "</hazelcast-client>";
    private static final String HAZELCAST_CLIENT_FAILOVER_START_TAG =
            "<hazelcast-client-failover xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";
    private static final String HAZELCAST_CLIENT_FAILOVER_END_TAG = "</hazelcast-client-failover>";

    private List<String> testConfigPaths = new LinkedList<>();

    // MEMBER

    public File givenXmlConfigFileInWorkDir(String instanceName) throws Exception {
        return givenXmlConfigFileInWorkDir("hazelcast.xml", instanceName);
    }

    public File givenXmlConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = xmlConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public File givenYamlConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlConfigFileInWorkDir("hazelcast.yaml", instanceName);
    }

    public File givenYmlConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlConfigFileInWorkDir("hazelcast.yml", instanceName);
    }

    public File givenYamlConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = yamlConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenXmlConfigFileOnClasspath(String instanceName) throws Exception {
        return givenXmlConfigFileOnClasspath("hazelcast.xml", instanceName);
    }

    public URL givenXmlConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String xml = xmlConfig(instanceName);
        return givenConfigFileOnClasspath(filename, xml);
    }

    public URL givenYamlConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlConfigFileOnClasspath("hazelcast.yaml", instanceName);
    }

    public URL givenYmlConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlConfigFileOnClasspath("hazelcast.yml", instanceName);
    }

    public URL givenYamlConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String yaml = yamlConfig(instanceName);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    // CLIENT

    public File givenXmlClientConfigFileInWorkDir(String instanceName) throws Exception {
        return givenXmlClientConfigFileInWorkDir("hazelcast-client.xml", instanceName);
    }

    public File givenXmlClientConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = xmlClientConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenXmlClientConfigFileOnClasspath(String instanceName) throws Exception {
        return givenXmlClientConfigFileOnClasspath("hazelcast-client.xml", instanceName);
    }

    public URL givenXmlClientConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String xml = xmlClientConfig(instanceName);
        return givenConfigFileOnClasspath(filename, xml);
    }

    public File givenYamlClientConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlClientConfigFileInWorkDir("hazelcast-client.yaml", instanceName);
    }

    public File givenYmlClientConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlClientConfigFileInWorkDir("hazelcast-client.yml", instanceName);
    }

    public File givenYamlClientConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = yamlClientConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenYamlClientConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlClientConfigFileOnClasspath("hazelcast-client.yaml", instanceName);
    }

    public URL givenYmlClientConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlClientConfigFileOnClasspath("hazelcast-client.yml", instanceName);
    }

    public URL givenYamlClientConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String yaml = yamlClientConfig(instanceName);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    // CLIENT-FAILOVER

    public File givenXmlClientFailoverConfigFileInWorkDir(int tryCount) throws Exception {
        return givenXmlClientFailoverConfigFileInWorkDir("hazelcast-client-failover.xml", tryCount);
    }

    public File givenXmlClientFailoverConfigFileInWorkDir(String filename, int tryCount) throws Exception {
        String xml = xmlFailoverClientConfig(tryCount);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenXmlClientFailoverConfigFileOnClasspath(int tryCount) throws Exception {
        return givenXmlClientFailoverConfigFileOnClasspath("hazelcast-client-failover.xml", tryCount);
    }

    public URL givenXmlClientFailoverConfigFileOnClasspath(String filename, int tryCount) throws Exception {
        String xml = xmlFailoverClientConfig(tryCount);
        return givenConfigFileOnClasspath(filename, xml);
    }

    public File givenYamlClientFailoverConfigFileInWorkDir(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileInWorkDir("hazelcast-client-failover.yaml", tryCount);
    }

    public File givenYmlClientFailoverConfigFileInWorkDir(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileInWorkDir("hazelcast-client-failover.yml", tryCount);
    }

    public File givenYamlClientFailoverConfigFileInWorkDir(String filename, int tryCount) throws Exception {
        String xml = yamlFailoverClientConfig(tryCount);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenYamlClientFailoverConfigFileOnClasspath(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileOnClasspath("hazelcast-client-failover.yaml", tryCount);
    }

    public URL givenYmlClientFailoverConfigFileOnClasspath(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileOnClasspath("hazelcast-client-failover.yml", tryCount);
    }

    public URL givenYamlClientFailoverConfigFileOnClasspath(String filename, int tryCount) throws Exception {
        String yaml = yamlFailoverClientConfig(tryCount);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    public File givenConfigFileInWorkDir(String filename, String content) throws IOException {
        File file = new File(filename);
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.print(content);
        writer.close();

        testConfigPaths.add(file.getAbsolutePath());

        return file;
    }

    public URL givenConfigFileOnClasspath(String filename, String content) throws Exception {
        URL classPathConfigUrl = Config.class.getClassLoader().getResource(".");
        String configFilePath = classPathConfigUrl.getFile() + "/" + filename;
        File file = new File(configFilePath);
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(content);
        writer.close();

        testConfigPaths.add(file.getAbsolutePath());

        return getClass().getClassLoader().getResource(filename);
    }

    public String createFilesWithCycleImports(Function<String, String> fileContentWithImportResource, String... paths) throws Exception {
        for (int i = 1; i < paths.length; i++) {
            createFileWithDependencyImport(paths[i - 1], paths[i], fileContentWithImportResource);
        }
        return createFileWithDependencyImport(paths[0], paths[1], fileContentWithImportResource);
    }

    private String createFileWithDependencyImport(
            String dependent,
            String pathToDependency,
            Function<String, String> fileContentWithImportResource) throws Exception {
        final String xmlContent = fileContentWithImportResource.apply(pathToDependency);
        givenConfigFileInWorkDir(dependent, xmlContent);
        return xmlContent;
    }

    private String xmlConfig(String instanceName) {
        return ""
                + HAZELCAST_START_TAG
                + "  <instance-name>" + instanceName + "</instance-name>"
                + HAZELCAST_END_TAG;
    }

    private String xmlClientConfig(String instanceName) {
        return ""
                + HAZELCAST_CLIENT_START_TAG
                + "  <instance-name>" + instanceName + "</instance-name>"
                + "  <connection-strategy async-start=\"true\" >"
                + "  </connection-strategy>"
                + HAZELCAST_CLIENT_END_TAG;
    }

    private String xmlFailoverClientConfig(int tryCount) {
        return ""
                + HAZELCAST_CLIENT_FAILOVER_START_TAG
                + "  <try-count>" + tryCount + "</try-count>"
                + "  <clients>"
                + "    <client>hazelcast-client-c1.xml</client>"
                + "  </clients>"
                + HAZELCAST_CLIENT_FAILOVER_END_TAG;
    }

    private String yamlConfig(String instanceName) {
        return ""
                + "hazelcast:\n"
                + "  instance-name: " + instanceName;
    }

    private String yamlClientConfig(String instanceName) {
        return ""
                + "hazelcast-client:\n"
                + "  instance-name: " + instanceName + "\n"
                + "  connection-strategy:\n"
                + "    async-start: true\n";
    }

    private String yamlFailoverClientConfig(int tryCount) {
        return ""
                + "hazelcast-client-failover:\n"
                + "  try-count: " + tryCount + "\n"
                + "  clients:\n"
                + "    - hazelcast-client-c1.yaml";
    }

    public void ensureTestConfigDeleted() {
        if (!testConfigPaths.isEmpty()) {
            for (String testConfigPath : testConfigPaths) {
                File file = new File(testConfigPath);
                file.delete();
            }
        }
    }
}
