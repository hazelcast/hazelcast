/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.console;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.console.ConsoleApp;
import com.hazelcast.core.HazelcastInstance;


/**
 * A demo application to demonstrate a Hazelcast client. This is probably NOT something you want to use in production.
 */
public class ClientConsoleApp extends ConsoleApp {

  public ClientConsoleApp(HazelcastInstance hazelcast) {
    super(hazelcast);
  }

  /**
   * Starts the test application. Loads the config from classpath hazelcast.xml,
   * if it fails to load, will use default config.
   *
   * @param args none
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ClientConfig clientConfig;

    try {
      clientConfig = new XmlClientConfigBuilder().build();
    } catch (IllegalArgumentException e) {
      clientConfig = new ClientConfig();
    }
    final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
    ClientConsoleApp clientConsoleApp = new ClientConsoleApp(client);
    clientConsoleApp.start();
  }

}
