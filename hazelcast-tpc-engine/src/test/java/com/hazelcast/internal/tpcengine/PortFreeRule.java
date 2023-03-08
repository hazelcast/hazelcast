/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.OS;
import org.junit.rules.ExternalResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.fail;

public class PortFreeRule extends ExternalResource {
    private final int port;

    public PortFreeRule(int port) {
        this.port = port;
    }

    @Override
    protected void before() throws Throwable {
        assertPortFree();
    }

    @Override
    protected void after() {
        assertPortFree();
    }

    private void assertPortFree() {
         if(!OS.isLinux()){
            // Functionality only available under Linux.
            return;
        }

        try {
            Process process = new ProcessBuilder("lsof", "-i", "-P","-n","tcp:" + port).start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            int exitCode = process.waitFor();

            // nothing was found.
            if (exitCode == 1) {
                System.out.println("Port "+port+" is free");
                return;
            }

            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
                result.append(System.getProperty("line.separator"));
            }
          //  System.out.println("---------------["+check()+"]");

            Thread.sleep(100);
            fail("Port " + port + " is not properly closed: " + result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String check() throws IOException, InterruptedException {
        return execute("lsof", "-i", "-P","-n","tcp:" + port);
    }

    private static String execute(String... command) throws InterruptedException, IOException {
        Process process = new ProcessBuilder(command).start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        int exitCode = process.waitFor();
        if (exitCode == 1) {
            return null;
        }

        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            result.append(line);
            result.append(System.getProperty("line.separator"));
        }
        return result.toString();
    }
}
