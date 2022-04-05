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

package com.hazelcast.jet.test;

import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;

import java.io.File;
import java.net.URL;

public class JetDeclarativeConfigFileHelper extends DeclarativeConfigFileHelper {

    public URL givenYamlJetConfigFileOnClasspath(String filename, String property, String value) throws Exception {
        String yaml = yamlJetConfigWithProperty(property, value);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    public URL givenYmlJetConfigFileOnClasspath(String filename, String property, String value) throws Exception {
        String yaml = yamlJetConfigWithProperty(property, value);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    public File givenYamlJetConfigFileOnWorkdir(String filename, String property, String value) throws Exception {
        String yaml = yamlJetConfigWithProperty(property, value);
        return givenConfigFileInWorkDir(filename, yaml);
    }

    public File givenYmlJetConfigFileOnWorkdir(String filename, String property, String value) throws Exception {
        String yaml = yamlJetConfigWithProperty(property, value);
        return givenConfigFileInWorkDir(filename, yaml);
    }

    public File givenXmlJetConfigFileOnWorkdir(String filename, String property, String value) throws Exception {
        String xml = xmlJetConfigWithProperty(property, value);
        return givenConfigFileInWorkDir(filename, xml);
    }

    private String yamlJetConfigWithProperty(String property, String value) {
        return "hazelcast-jet:\n" +
                "  properties:\n" +
                "    " + property + ": " + value + "\n";
    }

    private String xmlJetConfigWithProperty(String property, String value) {
        return "<hazelcast-jet xmlns=\"http://www.hazelcast.com/schema/jet-config\">\n<properties>\n" +
                "<property name=\"" + property + "\">" + value + "</property>\n</properties>\n" +
                "</hazelcast-jet>";
    }
}
