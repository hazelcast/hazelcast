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

package com.hazelcast.osgi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.AbstractHazelcastInstanceProxy;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;

/**
 * {@link com.hazelcast.osgi.HazelcastOSGiInstance} implementation
 * as proxy of delegated {@link com.hazelcast.core.HazelcastInstance} for getting from OSGi service.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
class HazelcastOSGiInstanceImpl extends AbstractHazelcastInstanceProxy
        implements HazelcastOSGiInstance {

    private final HazelcastOSGiService ownerService;

    HazelcastOSGiInstanceImpl(HazelcastInstance target,
                              HazelcastOSGiService ownerService) {
        super(target);
        this.ownerService = ownerService;
    }

    @Override
    public HazelcastInstance getDelegatedInstance() {
        return target;
    }

    @Override
    public HazelcastOSGiService getOwnerService() {
        return ownerService;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HazelcastOSGiInstanceImpl that = (HazelcastOSGiInstanceImpl) o;

        if (!target.equals(that.target)) {
            return false;
        }
        if (!ownerService.equals(that.ownerService)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = ownerService.hashCode();
        result = 31 * result + target.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("HazelcastOSGiInstanceImpl");
        sb.append("{delegatedInstance='").append(target).append('\'');
        Config config = getConfig();
        if (!StringUtil.isNullOrEmpty(config.getClusterName())) {
            sb.append(", clusterName=").append(config.getClusterName());
        }
        sb.append(", ownerServiceId=").append(ownerService.getId());
        sb.append('}');
        return sb.toString();
    }
}
