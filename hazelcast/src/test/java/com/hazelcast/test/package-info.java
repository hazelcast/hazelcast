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

/**
 * <h4>Hazelcast Test Runners overview</h4>
 * <table>
 * <tr><th>Class</th><th>Parametric</th><th>Parallel method execution</th></tr>
 * <tr>
 * <td>{@link com.hazelcast.test.HazelcastSerialClassRunner}</td>
 * <td>No</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>{@link com.hazelcast.test.HazelcastParallelClassRunner}</td>
 * <td>No</td>
 * <td>Yes</td>
 * </tr>
 * <tr>
 * <td>{@link com.hazelcast.test.HazelcastParametersRunnerFactory}</td>
 * <td>Yes</td>
 * <td>Based presence of the {@link com.hazelcast.test.annotation.ParallelTest} category</td>
 * </tr>
 * <tr>
 * <td>{@link com.hazelcast.test.HazelcastSerialParametersRunnerFactory}</td>
 * <td>Yes</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>{@link com.hazelcast.test.HazelcastParallelParametersRunnerFactory}</td>
 * <td>Yes</td>
 * <td>Yes</td>
 * </tr>
 * </table>
 */
package com.hazelcast.test;
