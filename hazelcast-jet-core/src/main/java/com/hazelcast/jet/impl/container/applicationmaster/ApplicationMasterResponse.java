/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.container.applicationmaster;

import com.hazelcast.jet.impl.container.ContainerResponse;

/**
 * Return application response;
 * SUCCESS - if application's execution was success;
 * FAILURE - if application's execution was not success;
 */
public interface ApplicationMasterResponse extends ContainerResponse {
    ApplicationMasterResponse SUCCESS = new ApplicationMasterResponse() {
        @Override
        public boolean isSuccess() {
            return true;
        }
    };
    ApplicationMasterResponse FAILURE = new ApplicationMasterResponse() {
        @Override
        public boolean isSuccess() {
            return false;
        }
    };

    /**
     * Indicates last state-machine transitions state;
     *
     * @return - true - execution was success , false - otherwise;
     */
    boolean isSuccess();
}
