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

package com.hazelcast.internal.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.spi.merge.MergingCreationTime;
import com.hazelcast.spi.merge.MergingHits;
import com.hazelcast.spi.merge.MergingLastAccessTime;
import com.hazelcast.spi.merge.MergingLastStoredTime;
import com.hazelcast.spi.merge.MergingLastUpdateTime;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.MergingView;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates a merge policy instance.
 */
public final class MergePolicyValidator {

    private MergePolicyValidator() {
    }

    /**
     * Checks the merge policy configuration
     * aligns with {@link MapConfig}.
     *
     * @param mapConfig            the {@link MapConfig}
     * @param mergePolicyClassName class name of merge policy
     * @param mergePolicyProvider  the {@link
     *                             SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkMapMergePolicy(MapConfig mapConfig,
                                           String mergePolicyClassName,
                                           SplitBrainMergePolicyProvider mergePolicyProvider) {
        SplitBrainMergePolicy mergePolicyInstance = mergePolicyProvider.getMergePolicy(mergePolicyClassName);
        List<Class> requiredMergeTypes =
                checkSplitBrainMergePolicy(SplitBrainMergeTypes.MapMergeTypes.class, mergePolicyInstance);
        if (!mapConfig.isPerEntryStatsEnabled() && requiredMergeTypes != null) {
            checkMapMergePolicyWhenStatisticsAreDisabled(mergePolicyClassName, requiredMergeTypes);
        }
    }

    /**
     * Checks if the configured merge policy requires merge types,
     * which are just available if map statistics are enabled.
     *
     * @param mergePolicyClass   the name of the configured merge policy class
     * @param requiredMergeTypes the required merge types of the configured merge policy
     */
    private static void checkMapMergePolicyWhenStatisticsAreDisabled(String mergePolicyClass,
                                                                     List<Class> requiredMergeTypes) {
        for (Class<?> requiredMergeType : requiredMergeTypes) {
            if (MergingLastStoredTime.class.isAssignableFrom(requiredMergeType)
                    || MergingCreationTime.class.isAssignableFrom(requiredMergeType)
                    || MergingHits.class.isAssignableFrom(requiredMergeType)
                    || MergingLastUpdateTime.class.isAssignableFrom(requiredMergeType)
                    || MergingLastAccessTime.class.isAssignableFrom(requiredMergeType)) {
                throw new InvalidConfigurationException("The merge policy " + mergePolicyClass
                        + " requires the merge type " + requiredMergeType.getName()
                        + ", which is just provided if perEntryStatsEnabled field of map-config is true.");
            }
        }
    }

    /**
     * Checks if the provided {@code mergeTypes} satisfy the requirements of a
     * given merge policy.
     *
     * @param mergeTypes           the provided merge types
     * @param mergePolicyProvider  the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     * @param mergePolicyClassName the merge policy class name
     * @throws InvalidConfigurationException if the given merge policy is no {@link SplitBrainMergePolicy}
     */
    static void checkMergeTypeProviderHasRequiredTypes(Class<? extends MergingValue> mergeTypes,
                                                       SplitBrainMergePolicyProvider mergePolicyProvider,
                                                       String mergePolicyClassName) {
        if (mergePolicyProvider == null) {
            return;
        }

        SplitBrainMergePolicy mergePolicy = getMergePolicyInstance(mergePolicyProvider,
                mergePolicyClassName);
        checkSplitBrainMergePolicy(mergeTypes, mergePolicy);
    }

    private static SplitBrainMergePolicy getMergePolicyInstance(SplitBrainMergePolicyProvider mergePolicyProvider,
                                                                String mergePolicyClassName) {
        try {
            return mergePolicyProvider.getMergePolicy(mergePolicyClassName);
        } catch (InvalidConfigurationException e) {
            throw new InvalidConfigurationException("Merge policy must be an instance of SplitBrainMergePolicy,"
                    + " but was " + mergePolicyClassName, e.getCause());
        }
    }

    /**
     * Checks if the provided {@code mergeTypes} satisfy the requirements of a
     * given merge policy.
     *
     * @param mergeTypes          the provided merge types
     * @param mergePolicyInstance the {@link SplitBrainMergePolicy} instance
     * @return a list of the required merge types
     */
    private static List<Class> checkSplitBrainMergePolicy(Class<? extends MergingValue> mergeTypes,
                                                          SplitBrainMergePolicy mergePolicyInstance) {
        List<Class> requiredMergeTypes = new ArrayList<>();
        Class<?> mergePolicyClass = mergePolicyInstance.getClass();
        String mergePolicyClassName = mergePolicyClass.getName();
        // iterate over the complete class hierarchy of a merge policy, to check all its generics
        do {
            checkSplitBrainMergePolicyGenerics(requiredMergeTypes, mergeTypes, mergePolicyClassName, mergePolicyClass);
            mergePolicyClass = mergePolicyClass.getSuperclass();
        } while (mergePolicyClass != null);
        return requiredMergeTypes;
    }

    private static void checkSplitBrainMergePolicyGenerics(List<Class> requiredMergeTypes, Class providedMergeTypes,
                                                           String mergePolicyClassName, Class<?> mergePolicyClass) {
        for (TypeVariable<? extends Class<?>> classTypeVariable : mergePolicyClass.getTypeParameters()) {
            // checks merge policies like
            // CustomMergePolicy<V, T extends MergingValue<V>> implements SplitBrainMergePolicy<V, T>
            for (Type requireMergeType : classTypeVariable.getBounds()) {
                checkRequiredMergeType(requiredMergeTypes, providedMergeTypes, mergePolicyClassName, requireMergeType);
            }
        }

        for (Type type : mergePolicyClass.getGenericInterfaces()) {
            // checks merge policies like
            // CustomMergePolicy implements SplitBrainMergePolicy<Object, SplitBrainMergeTypes$...MergeTypes>
            checkRequiredGenericType(requiredMergeTypes, providedMergeTypes, mergePolicyClassName, type);
        }

        // checks merge policies like
        // CustomMergePolicy extends AbstractSplitBrainMergePolicy<Object, SplitBrainMergeTypes$...MergeTypes>
        Type type = mergePolicyClass.getGenericSuperclass();
        checkRequiredGenericType(requiredMergeTypes, providedMergeTypes, mergePolicyClassName, type);
    }

    private static void checkRequiredGenericType(List<Class> requiredMergeTypes, Class providedMergeTypes,
                                                 String mergePolicyClassName, Type requiredMergeType) {
        if (requiredMergeType instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) requiredMergeType).getActualTypeArguments();
            for (Type requireMergeType : actualTypeArguments) {
                checkRequiredMergeType(requiredMergeTypes, providedMergeTypes, mergePolicyClassName, requireMergeType);
            }
        }
    }

    private static void checkRequiredMergeType(List<Class> requiredMergeTypes, Class providedMergeTypes,
                                               String mergePolicyClassName, Type requireMergeType) {
        if (requireMergeType instanceof ParameterizedType) {
            // checks types like Merging...<V> extends MergingValue<V>
            Class<?> requiredMergeType = (Class<?>) ((ParameterizedType) requireMergeType).getRawType();
            checkRequiredMergeTypeClass(requiredMergeTypes, providedMergeTypes, mergePolicyClassName, requiredMergeType);
        } else if (requireMergeType instanceof Class) {
            // checks types like SplitBrainMergeTypes$...MergeTypes
            Class<?> requiredMergeType = (Class) requireMergeType;
            checkRequiredMergeTypeClass(requiredMergeTypes, providedMergeTypes, mergePolicyClassName, requiredMergeType);
        }
    }

    private static void checkRequiredMergeTypeClass(List<Class> requiredMergeTypes, Class providedMergeTypes,
                                                    String mergePolicyClassName, Class<?> requiredMergeTypeClass) {
        if (!MergingView.class.isAssignableFrom(requiredMergeTypeClass)) {
            // just check types, which inherit from MergingView
            return;
        }
        if (!requiredMergeTypeClass.isAssignableFrom(providedMergeTypes)) {
            throw new InvalidConfigurationException("The merge policy " + mergePolicyClassName
                    + " can just be configured on data structures which provide the merging type "
                    + requiredMergeTypeClass.getName()
                    + ". See SplitBrainMergeTypes for supported merging types.");
        }
        requiredMergeTypes.add(requiredMergeTypeClass);
    }
}
