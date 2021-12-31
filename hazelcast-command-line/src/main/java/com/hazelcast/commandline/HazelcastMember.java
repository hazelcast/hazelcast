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

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigStream;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.FileSystemYamlConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.config.MemberXmlConfigRootTagRecognizer;
import com.hazelcast.internal.config.MemberYamlConfigRootTagRecognizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Class for starting new Hazelcast members
 */
final class HazelcastMember {
    private HazelcastMember() {
    }

    /**
     * Creates a server instance of Hazelcast.
     * <p>
     * If user sets the system property "print.port", the server writes the port number of the Hazelcast instance to a file.
     * The file name is the same as the "print.port" property.
     *
     * @param args none
     */
    public static void main(String[] args)
            throws Exception {
        System.setProperty("hazelcast.tracking.server", "true");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config());
        printMemberPort(hz);
    }

    private static void printMemberPort(HazelcastInstance hz) throws FileNotFoundException, UnsupportedEncodingException {
        String printPort = System.getProperty("print.port");
        if (printPort != null) {
            try (PrintWriter printWriter = new PrintWriter("ports" + File.separator + printPort, "UTF-8")) {
                printWriter.println(hz.getCluster().getLocalMember().getAddress().getPort());
            }
        }
    }

    static Config config()
            throws Exception {
        String hazelcastConfig = System.getProperty("hazelcast.config");
        Config config;
        if (!isNullOrEmpty(hazelcastConfig)) {
            config = createConfig(hazelcastConfig);
        } else {
            config = Config.load();
        }
        config.getJetConfig().setEnabled(true);
        String port = System.getProperty("network.port");
        if (port != null && !port.equalsIgnoreCase("null")) {
            config.getNetworkConfig().setPort(Integer.parseInt(port));
        }
        String networkInterface = System.getProperty("network.interface");
        if (networkInterface != null && !networkInterface.equalsIgnoreCase("null")) {
            config.setProperty("hazelcast.socket.bind.any", "false");
            InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
            interfaces.setEnabled(true).addInterface(networkInterface);
        }
        return config;
    }

    private static Config createConfig(String configPath)
            throws Exception {
        if (new MemberYamlConfigRootTagRecognizer().isRecognized(new ConfigStream(new FileInputStream(configPath)))) {
            return new FileSystemYamlConfig(configPath);
        } else if (new MemberXmlConfigRootTagRecognizer().isRecognized(new ConfigStream(new FileInputStream(configPath)))) {
            return new FileSystemXmlConfig(configPath);
        } else {
            throw new InvalidConfigurationException("Provided configuration file is invalid.");
        }
    }
}
