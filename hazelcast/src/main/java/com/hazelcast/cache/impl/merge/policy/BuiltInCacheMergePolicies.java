/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.merge.policy;

/**
 * <p>
 * Enum that represents all built-in {@link CacheMergePolicy} implementations.
 * </p>
 *
 * <p>
 * Note: When a new built-in {@link CacheMergePolicy} is implemented, its definition should be added here also.
 * </p>
 */
public enum BuiltInCacheMergePolicies {

    /**
     * Cache merge policy that merges cache entry to from source to destination directly.
     */
    PASS_THROUGH(PassThroughCacheMergePolicy.class, new CacheMergePolicyInstanceFactory() {
        @Override
        public CacheMergePolicy create() {
            return new PassThroughCacheMergePolicy();
        }
    }),

    /**
     * Cache merge policy that merges cache entry to from source to destination
     * if it does not exist in the destination cache.
     */
    PUT_IF_ABSENT(PutIfAbsentCacheMergePolicy.class, new CacheMergePolicyInstanceFactory() {
        @Override
        public CacheMergePolicy create() {
            return new PutIfAbsentCacheMergePolicy();
        }
    }),

    /**
     * Cache merge policy that merges cache entry from source to destination cache
     * if source entry has more hits than the destination one.
     */
    HIGHER_HITS(HigherHitsCacheMergePolicy.class, new CacheMergePolicyInstanceFactory() {
        @Override
        public CacheMergePolicy create() {
            return new HigherHitsCacheMergePolicy();
        }
    }),

    /**
     * Cache merge policy that merges cache entry from source to destination cache
     * if source entry has been accessed more recently than the destination entry.
     */
    LATEST_ACCESS(LatestAccessCacheMergePolicy.class, new CacheMergePolicyInstanceFactory() {
        @Override
        public CacheMergePolicy create() {
            return new LatestAccessCacheMergePolicy();
        }
    });

    private Class<? extends CacheMergePolicy> implClass;
    private CacheMergePolicyInstanceFactory instanceFactory;

    BuiltInCacheMergePolicies(Class<? extends CacheMergePolicy> implClass,
                              CacheMergePolicyInstanceFactory instanceFactory) {
        this.implClass = implClass;
        this.instanceFactory = instanceFactory;
    }

    public Class<? extends CacheMergePolicy> getImplementationClass() {
        return implClass;
    }

    /**
     * Gets the name of the implementation class of {@link CacheMergePolicy}.
     *
     * @return the name of the implementation class of {@link CacheMergePolicy}.
     */
    public String getName() {
        return implClass.getName();
    }

    /**
     * Create a new instance of {@link CacheMergePolicy}.
     *
     * @return the created instance of {@link CacheMergePolicy}
     */
    public CacheMergePolicy newInstance() {
        return instanceFactory.create();
    }

    /**
     * Gets the definition of the default {@link CacheMergePolicy}.
     *
     * @return the definition of the default {@link CacheMergePolicy}
     */
    public static BuiltInCacheMergePolicies getDefault() {
        return PASS_THROUGH;
    }

    private interface CacheMergePolicyInstanceFactory {
        CacheMergePolicy create();
    }

}
