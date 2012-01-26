/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.examples;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.examples.TestApp;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestClientApp {

    HazelcastClient hz;

    TestApp app;

    public static void main(String[] arguments) {
        new TestClientApp().run();
    }

    public void run() {
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        message();
        while (true) {
            System.out.print("client > ");
            try {
                final String command = in.readLine();
                String[] argsSplit = command.split(" ");
                String[] args = new String[argsSplit.length];
                for (int i = 0; i < argsSplit.length; i++) {
                    args[i] = argsSplit[i].trim();
                }
                handleCommand(args);
                if (hz != null) {
                    app = new TestApp(hz);
                    app.start(null);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private void handleCommand(String[] args) throws Exception {
        if (args[0].startsWith("connect")) {
            connect(args);
        } else {
            if (hz == null) {
                message();
                return;
            } else {
                System.out.println("Unknown command. Sample commands:");
                System.out.println("connect 192.168.1.3");
            }
        }
    }

    private void message() {
        System.out.println("Make sure you started Hazelcast server first.");
        System.out.println("You should connect first by typing 'connect <hazelcast-server-ip> <group-name> <group-password>'");
        System.out.println("If group-name is 'dev' and password is 'dev-pass', 'connect <hazelcast-server-ip>' will be enough ");
    }

    private void connect(String[] args) {
        String ip = "localhost";
        String groupName = "dev";
        String pass = "dev-pass";
        if (args.length > 1) {
            ip = args[1];
        }
        if (args.length > 3) {
            groupName = args[2];
            pass = args[3];
        }
        System.out.println("Connecting to " + ip);
        String[] ips = null;
        if (ip.indexOf(':') == -1) {
            ips = new String[]{ip + ":5701", ip + ":5702", ip + ":5703"};
        } else {
            ips = new String[]{ip};
        }
        this.hz = HazelcastClient.newHazelcastClient(groupName, pass, ips);
        System.out.println(hz.getCluster().getMembers());
    }
}
