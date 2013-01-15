/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.examples;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;

import java.io.IOException;
import java.io.Serializable;

public class ChatApplication {

    private String username;
    private final IMap<String, ChatMessage> map = Hazelcast.newHazelcastInstance(null).getMap("chat-application");

    public static void main(String[] args) {
        ChatApplication application = new ChatApplication();
        String username = (args != null && args.length > 0) ? args[0] : null;
        if (username == null) {
            System.out.println("enter username:");
            int input;
            StringBuilder u = new StringBuilder();
            try {
                while ((input = System.in.read()) != '\n')
                    u.append((char) input);
            } catch (IOException e) {
                e.printStackTrace();
            }
            username = u.toString();
        }
        System.out.println("hello " + username);
        application.setUsername(username);
        application.run();
    }

    public void setUsername(String name) {
        this.username = name;
        new ChatMessage(username, "has joined").send(map);
    }

    public void run() {
        boolean chatting = true;
        showConnected(map);
        map.addEntryListener(new ChatCallback(), true);
        while (chatting) {
            int input;
            StringBuilder message = new StringBuilder();
            ChatMessage chat;
            try {
                while ((input = System.in.read()) != '\n')
                    message.append((char) input);
            } catch (IOException e) {
                e.printStackTrace();
            }
            chat = new ChatMessage(username, message.toString());
            chat.send(map);
        }
    }

    private void showConnected(IMap<String, ChatMessage> map) {
        for (String user : map.keySet()) {
            System.out.println(user + " is online");
        }
    }

    private static class ChatMessage implements Serializable {
        private String username;
        private String message;

        public ChatMessage(String username, String message) {
            this.username = username;
            this.message = message;
        }

        public String toString() {
            return username + ": " + message;
        }

        public void send(IMap<String, ChatMessage> map) {
            map.put(username, this);
        }
    }

    private class ChatCallback implements EntryListener {
        public ChatCallback() {
        }

        public void entryAdded(EntryEvent event) {
            if (!username.equals(event.getKey())) {
                System.out.println(event.getValue());
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (!username.equals(event.getKey())) {
                System.out.println(event.getKey() + " left");
            }
        }

        public void entryUpdated(EntryEvent event) {
            if (!username.equals(event.getKey())) {
                System.out.println(event.getValue().toString());
            }
        }

        public void entryEvicted(EntryEvent event) {
        }
    }
}
