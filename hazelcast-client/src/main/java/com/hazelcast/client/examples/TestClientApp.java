/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client.examples;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.examples.TestApp;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestClientApp {

    HazelcastClient hz;

    TestApp app;

    public static void main(String[] arguments) {
        TestClientApp app = new TestClientApp();
        if (arguments.length == 0)
            app.noArgument();
        else
            try {
                app.connect(arguments);
                app.run(arguments);
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

    public void noArgument() {
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
                run(args);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    void run(String[] args) throws Exception {
        if (hz != null) {
            app = new TestApp(hz);
            app.start(null);
        }
    }

    private void handleCommand(String[] args) throws Exception {
        if (args[0].startsWith("connect")) {
            connect(copyOfRange(args, 1, args.length));
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

    private static String[] copyOfRange(String[] original, int from, int to) {
        int length = to - from;
        if (length < 0)
            throw new IllegalArgumentException(from + " > " + to);
        String[] copy = new String[length];
        System.arraycopy(original, from, copy, 0,
                Math.min(original.length - from, length));
        return copy;
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
        if (args.length > 0) {
            ip = args[0];
        }
        if (args.length > 2) {
            groupName = args[1];
            pass = args[2];
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
