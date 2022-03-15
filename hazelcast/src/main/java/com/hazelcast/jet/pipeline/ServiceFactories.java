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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import javax.annotation.Nonnull;

import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

/**
 * Utility class with methods that create several useful {@link ServiceFactory
 * service factories}.
 *
 * @since Jet 3.0
 */
public final class ServiceFactories {

    private ServiceFactories() { }

    /**
     * Returns a factory that provides a {@link ReplicatedMap} as the service
     * object. A replicated map is a particularly good choice if you are
     * enriching an event stream with the data stored in the Hazelcast Jet
     * cluster. Unlike in a {@code hashJoin} transformation, the data in the
     * map can change while the job is running so you can keep the enriching
     * dataset up-to-date. Unlike {@code IMap}, the data you access is local so
     * you won't do any blocking calls using it (important for performance).
     * <p>
     * If you want to destroy the map after the job finishes, call
     * {@code factory.destroyFn(ReplicatedMap::destroy)} on the object you get
     * from this method.
     * <p>
     * Example usage (without destroyFn):
     * <pre>
     * p.readFrom( /* a batch or streaming source &#42;/ )
     *  .mapUsingService(replicatedMapService("fooMapName"),
     *      (map, item) -> tuple2(item, map.get(item.getKey())))
     *  .destroyFn(ReplicatedMap::destroy);
     * </pre>
     *
     * @param mapName name of the {@link ReplicatedMap} to use as the service
     * @param <K> type of the map key
     * @param <V> type of the map value
     *
     * @since Jet 3.0
     */
    @Nonnull
    public static <K, V> ServiceFactory<?, ReplicatedMap<K, V>> replicatedMapService(@Nonnull String mapName) {
        ServiceFactory<?, ReplicatedMap<K, V>> replicatedMapFactory =
                ServiceFactories.sharedService(SecuredFunctions.replicatedMapFn(mapName));
        return replicatedMapFactory.withPermission(new ReplicatedMapPermission(mapName, ACTION_CREATE, ACTION_READ));
    }

    /**
     * Returns a factory that provides an {@link IMap} as the service. This
     * is useful if you are enriching an event stream with the data stored in
     * the Hazelcast Jet cluster. Unlike in a {@code hashJoin} transformation,
     * the data in the map can change while the job is running so you can keep
     * the enriching dataset up-to-date.
     * <p>
     * Instead of using this factory, you can call {@link
     * GeneralStage#mapUsingIMap(IMap, FunctionEx, BiFunctionEx)} or {@link
     * GeneralStageWithKey#mapUsingIMap(IMap, BiFunctionEx)}.
     * <p>
     * If you plan to use a sync method on the map, call {@link
     * ServiceFactory#toNonCooperative()} on the returned factory.
     *
     * @param mapName name of the map used as service
     * @param <K> key type
     * @param <V> value type
     * @return the service factory
     *
     * @since Jet 3.0
     */
    @Nonnull
    public static <K, V> ServiceFactory<?, IMap<K, V>> iMapService(@Nonnull String mapName) {
        ServiceFactory<?, IMap<K, V>> iMapServiceFactory =
                ServiceFactories.sharedService(SecuredFunctions.iMapFn(mapName));
        return iMapServiceFactory.withPermission(new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
    }

    /**
     * A variant of {@link #sharedService(FunctionEx, ConsumerEx)
     * sharedService(createFn, destroyFn)} with a no-op {@code
     * destroyFn}.
     * <p>
     * <strong>Note:</strong> if your service has a blocking API (e.g., doing
     * synchronous IO or acquiring locks), you must call {@link
     * ServiceFactory#toNonCooperative()} as a hint to the Jet execution engine
     * to start a dedicated thread for those calls. Failing to do this can
     * cause severe performance problems. You should also carefully consider
     * how much local parallelism you need for this step since each parallel
     * tasklet needs its own thread. Call {@link GeneralStage#setLocalParallelism
     * stage.setLocalParallelism()} to set an explicit level, otherwise it will
     * depend on the number of cores on the Jet machine, which makes no sense
     * for blocking code.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <S> ServiceFactory<?, S> sharedService(
            @Nonnull FunctionEx<? super ProcessorSupplier.Context, S> createServiceFn
    ) {
        return sharedService(createServiceFn, ConsumerEx.noop());
    }

    /**
     * Returns a {@link ServiceFactory} which will provide a single shared
     * service object per cluster member. All parallel processors serving the
     * associated pipeline stage will use the same object. Since the service
     * object will be accessed from many parallel threads, it must be
     * thread-safe.
     * <p>
     * <strong>Note:</strong> if your service has a blocking API (e.g., doing
     * synchronous IO or acquiring locks), you must call {@link
     * ServiceFactory#toNonCooperative()} as a hint to the Jet execution engine
     * to start a dedicated thread for those calls. Failing to do this can
     * cause severe performance problems. You should also carefully consider
     * how much local parallelism you need for this step since each parallel
     * tasklet needs its own thread. Call {@link GeneralStage#setLocalParallelism
     * stage.setLocalParallelism()} to set an explicit level, otherwise it will
     * depend on the number of cores on the Jet machine, which makes no sense
     * for blocking code.
     *
     * @param createServiceFn the function that creates the service. It will be called once on each
     *                        Jet member. It must be stateless.
     * @param destroyServiceFn the function that destroys the service. It will be called once on each
     *                         Jet member. It can be used to tear down any resources acquired by the
     *                         service. It must be stateless.
     * @param <S> type of the service object
     *
     * @see #nonSharedService(FunctionEx, ConsumerEx)
     */
    public static <S> ServiceFactory<?, S> sharedService(
            @Nonnull FunctionEx<? super ProcessorSupplier.Context, S> createServiceFn,
            @Nonnull ConsumerEx<S> destroyServiceFn
    ) {
        return ServiceFactory.withCreateContextFn(createServiceFn)
                             .withCreateServiceFn((ctx, c) -> c)
                             .withDestroyContextFn(destroyServiceFn);
    }

    /**
     * A variant of {@link #nonSharedService(FunctionEx, ConsumerEx)
     * nonSharedService(createFn, destroyFn)} with a no-op {@code
     * destroyFn}.
     * <p>
     * <strong>Note:</strong> if your service has a blocking API (e.g., doing
     * synchronous IO or acquiring locks), you must call {@link
     * ServiceFactory#toNonCooperative()} as a hint to the Jet execution engine
     * to start a dedicated thread for those calls. Failing to do this can
     * cause severe performance problems. You should also carefully consider
     * how much local parallelism you need for this step since each parallel
     * tasklet needs its own thread. Call {@link GeneralStage#setLocalParallelism
     * stage.setLocalParallelism()} to set an explicit level, otherwise it will
     * depend on the number of cores on the Jet machine, which makes no sense
     * for blocking code.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <S> ServiceFactory<?, S> nonSharedService(
            @Nonnull FunctionEx<? super Processor.Context, ? extends S> createServiceFn
    ) {
        return nonSharedService(createServiceFn, ConsumerEx.noop());
    }

    /**
     * Returns a {@link ServiceFactory} which creates a separate service
     * instance for each parallel Jet processor. The number of processors on
     * each cluster member is dictated by {@link GeneralStage#setLocalParallelism
     * stage.localParallelism}. Use this when the service instance should not
     * be shared across multiple threads.
     * <p>
     * <strong>Note:</strong> if your service has a blocking API (e.g., doing
     * synchronous IO or acquiring locks), you must call {@link
     * ServiceFactory#toNonCooperative()} as a hint to the Jet execution engine
     * to start a dedicated thread for those calls. Failing to do this can
     * cause severe performance problems. You should also carefully consider
     * how much local parallelism you need for this step since each parallel
     * tasklet needs its own thread. Call {@link GeneralStage#setLocalParallelism
     * stage.setLocalParallelism()} to set an explicit level, otherwise it will
     * depend on the number of cores on the Jet machine, which makes no sense
     * for blocking code.
     *
     * @param createServiceFn the function that creates the service. It will be called once per
     *                        processor instance. It must be stateless.
     * @param destroyServiceFn the function that destroys the service. It will be called once per
     *                         processor instance. It can be used to tear down any resources
     *                         acquired by the service. It must be stateless.
     *
     * @param <S> type of the service object
     *
     * @see #sharedService(FunctionEx, ConsumerEx)
     */
    public static <S> ServiceFactory<?, S> nonSharedService(
            @Nonnull FunctionEx<? super Processor.Context, ? extends S> createServiceFn,
            @Nonnull ConsumerEx<? super S> destroyServiceFn
    ) {
        return ServiceFactory.<Void>withCreateContextFn(c -> null)
                .<S>withCreateServiceFn(SecuredFunctions.createServiceFn(createServiceFn))
                .withDestroyServiceFn(destroyServiceFn);
    }
}
