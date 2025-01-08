/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

/**
 * Interface to be implemented by the classes that expose {@linkplain PhoneHomeMetrics
 * phone home data}. The {@link #provideMetrics(Node, MetricsCollectionContext)}
 * method is called in every metrics collection cycle.
 * <p>
 * Registering instances of this interface can be done by providing the class name in
 * {@code META-INF/services/com.hazelcast.internal.util.phonehome.MetricsProvider}.
 */
public interface MetricsProvider {

    int TIMEOUT = 2000;
    int RESPONSE_OK = HttpURLConnection.HTTP_OK;
    int RESPONSE_UNAUTHORIZED = HttpURLConnection.HTTP_UNAUTHORIZED;

    int A_INTERVAL = 5;
    int B_INTERVAL = 10;
    int C_INTERVAL = 20;
    int D_INTERVAL = 40;
    int E_INTERVAL = 60;
    int F_INTERVAL = 100;
    int G_INTERVAL = 150;
    int H_INTERVAL = 300;
    int J_INTERVAL = 600;

    /**
     * Metrics collection callback that is called in every metrics
     * collection cycle. The collected metrics should be passed to the
     * {@link MetricsCollectionContext#collect(Metric, Object)} method.
     *
     * @param node    this node
     * @param context the context used to collect the metrics
     */
    void provideMetrics(Node node, MetricsCollectionContext context);

    static String convertToLetter(int size) {
        String letter;
        if (size < A_INTERVAL) {
            letter = "A";
        } else if (size < B_INTERVAL) {
            letter = "B";
        } else if (size < C_INTERVAL) {
            letter = "C";
        } else if (size < D_INTERVAL) {
            letter = "D";
        } else if (size < E_INTERVAL) {
            letter = "E";
        } else if (size < F_INTERVAL) {
            letter = "F";
        } else if (size < G_INTERVAL) {
            letter = "G";
        } else if (size < H_INTERVAL) {
            letter = "H";
        } else if (size < J_INTERVAL) {
            letter = "J";
        } else {
            letter = "I";
        }
        return letter;
    }

    static boolean fetchWebService(String urlStr, int responseCode) {
        HttpURLConnection conn = null;
        try {
            URL url = URI.create(urlStr).toURL();
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(TIMEOUT);
            conn.setReadTimeout(TIMEOUT);
            conn.connect();
            return conn.getResponseCode() == responseCode;
        } catch (Exception ignored) {
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    static boolean fetchWebService(String url) {
        return fetchWebService(url, RESPONSE_OK);
    }
}
