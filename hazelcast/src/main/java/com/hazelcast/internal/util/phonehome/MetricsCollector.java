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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * Class responsible for collecting phone home data (phone home metrics).
 *
 * @see PhoneHomeMetrics
 */
interface MetricsCollector {

    int TIMEOUT = 2000;
    int RESPONSE_OK = 200;
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
     * Calls the {@code metricsConsumer} for each metric collected by this collector.
     *
     * @param node            this node
     * @param metricsConsumer the consumer to call with the metric type and value
     */
    void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer);

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

    static boolean fetchWebService(String urlStr) {
        HttpURLConnection conn = null;
        boolean response;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(TIMEOUT);
            conn.setReadTimeout(TIMEOUT);
            conn.connect();
            response = conn.getResponseCode() == RESPONSE_OK;
        } catch (Exception ignored) {
            ignore(ignored);
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return response;
    }
}
