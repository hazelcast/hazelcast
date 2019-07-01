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

package integration;

import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;

public class JmxIntegration {

    static void s1() {
        //tag::s1[]
        JetConfig jetConfig = new JetConfig();
        // use set-methods on this object
        MetricsConfig metricsConfig = jetConfig.getMetricsConfig();
        //end::s1[]
    }
}
