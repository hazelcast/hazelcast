/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.server;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.logging.LogManager;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * Main class that starts a Hazelcast Jet instance.
 */
public final class StartServer {

    private StartServer() {
    }

    /**
     * Creates a server instance of Hazelcast Jet. If the system property
     * {@code print.port} is set, the server writes the port number of the
     * Hazelcast instance to a file named by the property.
     */
    public static void main(String[] args) throws Exception {
        configureLogging();
        JetInstance jet = Jet.newJetInstance();
        printMemberPort(jet.getHazelcastInstance());
    }

    public static void configureLogging() throws IOException {
        InputStream input = StartServer.class.getClassLoader().getResourceAsStream("logging.properties");
        LogManager.getLogManager().readConfiguration(input);
    }

    private static void printMemberPort(HazelcastInstance hz) throws Exception {
        String printPort = System.getProperty("print.port");
        if (printPort != null) {
            PrintWriter printWriter = null;
            try {
                printWriter = new PrintWriter("ports" + File.separator + printPort, "UTF-8");
                printWriter.println(hz.getCluster().getLocalMember().getAddress().getPort());
            } finally {
                closeResource(printWriter);
            }
        }
    }
}
