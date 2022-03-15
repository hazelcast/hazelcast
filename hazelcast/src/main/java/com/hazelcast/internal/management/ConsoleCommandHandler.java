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

package com.hazelcast.internal.management;

import com.hazelcast.console.ConsoleApp;
import com.hazelcast.core.HazelcastInstance;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * Handler class for console commands that sent from Console application which located in Management Center.
 */
public class ConsoleCommandHandler {

    private final ConsoleHandlerApp app;
    private final Lock lock = new ReentrantLock();
    private final StringBuilder buffer = new StringBuilder();

    public ConsoleCommandHandler(HazelcastInstance instance) {
        this.app = new ConsoleHandlerApp(instance);
    }


   /**
    * Runs a command on the console. Will not run "exit", "quit", "shutdown", their upper-case
    * or mixed case counterparts.
    *
    * @param command The command to run.
    *
    * @return either the command is handled, or a console message is returned if the command is not handled.
    *
    * @throws java.lang.InterruptedException
    */
    public String handleCommand(final String command) throws InterruptedException {
        requireNonNull(command, "Command must not be null");

        if (lock.tryLock(1, TimeUnit.SECONDS)) {
            try {
                return doHandleCommand(command);
            } finally {
                lock.unlock();
            }
        }
        return "'" + command + "' execution is timed out!";
    }

   /**
    * Called by handleCommand.
    */
    String doHandleCommand(final String command) {
        app.handleCommand(command);
        final String output = buffer.toString();
        buffer.setLength(0);
        return output;
    }

    /**
     * Wrapper for {@link com.hazelcast.console.ConsoleApp}
     */
    private class ConsoleHandlerApp extends ConsoleApp {
        ConsoleHandlerApp(HazelcastInstance hazelcast) {
            super(hazelcast, System.out);
        }

        @Override
        protected void handleAddListener(String[] args) {
            println("Listener commands are not allowed!");
        }

        @Override
        protected void handleRemoveListener(String[] args) {
            println("Listener commands are not allowed!");
        }

        @Override
        public void println(Object obj) {
            print(obj);
            print('\n');
        }

        @Override
        public void print(Object obj) {
            buffer.append(obj);
        }

        @Override
        protected void handleExit() {
            print("'exit' is not allowed!");
        }

        @Override
        protected void handleShutdown() {
            print("'shutdown' is not allowed!");
        }
    }
}
