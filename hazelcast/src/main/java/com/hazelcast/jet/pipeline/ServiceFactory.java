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
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ProcessorSupplier.Context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.Serializable;
import java.security.Permission;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static java.util.Collections.emptyMap;

/**
 * A holder of functions needed to create and destroy a service object used in
 * pipeline transforms such as {@link GeneralStage#mapUsingService
 * stage.mapUsingService()}.
 * <p>
 * The lifecycle of this factory object is as follows:
 * <ol><li>
 *     When you submit a job, Jet serializes {@code ServiceFactory} and sends
 *     it to all the cluster members.
 * <li>
 *     On each member Jet calls {@link #createContextFn()} to get a context
 *     object that will be shared across all the service instances on that
 *     member. For example, if you are connecting to an external service that
 *     provides a thread-safe client, you can create it here and then create
 *     individual sessions for each service instance.
 * <li>
 *     Jet repeatedly calls {@link #createServiceFn()} to create as many
 *     service instances on each member as determined by the {@link
 *     GeneralStage#setLocalParallelism(int) localParallelism} of the pipeline
 *     stage. The invocations of {@link #createServiceFn()} receive the context
 *     object.
 *  <li>
 *      When the job is done, Jet calls {@link #destroyServiceFn()} with each
 *      service instance.
 *  <li>
 *      Finally, Jet calls {@link #destroyContextFn()} with the context object.
 * </ol>
 * If you don't need the member-wide context object, you can call the simpler
 * methods {@link ServiceFactories#nonSharedService(FunctionEx, ConsumerEx)}
 * ServiceFactories.processorLocalService} or {@link
 * ServiceFactories#sharedService(FunctionEx, ConsumerEx)}
 * ServiceFactories.memberLocalService}.
 * <p>
 * Here's a list of pipeline transforms that require a {@code ServiceFactory}:
 * <ul>
 *     <li>{@link GeneralStage#mapUsingService}
 *     <li>{@link GeneralStage#filterUsingService}
 *     <li>{@link GeneralStage#flatMapUsingService}
 *     <li>{@link GeneralStage#mapUsingServiceAsync}
 *     <li>{@link GeneralStage#mapUsingServiceAsyncBatched}
 *     <li>{@link GeneralStageWithKey#mapUsingService}
 *     <li>{@link GeneralStageWithKey#filterUsingService}
 *     <li>{@link GeneralStageWithKey#flatMapUsingService}
 *     <li>{@link GeneralStageWithKey#mapUsingServiceAsync}
 * </ul>
 *
 * @param <C> type of the shared context object
 * @param <S> type of the service object
 *
 * @since Jet 4.0
 */
@SerializableByConvention
public final class ServiceFactory<C, S> implements Serializable, Cloneable {

    /**
     * Default value for {@link #isCooperative}.
     */
    public static final boolean COOPERATIVE_DEFAULT = true;

    private static final long serialVersionUID = 1L;

    private boolean isCooperative = COOPERATIVE_DEFAULT;

    @Nonnull
    private final FunctionEx<? super Context, ? extends C> createContextFn;

    @Nonnull
    private BiFunctionEx<? super Processor.Context, ? super C, ? extends S> createServiceFn = (ctx, svcContext) -> {
        throw new IllegalStateException("This ServiceFactory is missing a createServiceFn");
    };

    @Nonnull
    private ConsumerEx<? super S> destroyServiceFn = ConsumerEx.noop();

    @Nonnull
    private ConsumerEx<? super C> destroyContextFn = ConsumerEx.noop();

    @Nonnull
    private Map<String, File> attachedFiles = emptyMap();

    @Nullable
    private Permission permission;

    private ServiceFactory(@Nonnull FunctionEx<? super ProcessorSupplier.Context, ? extends C> createContextFn) {
        this.createContextFn = createContextFn;
    }

    /**
     * Creates a new {@code ServiceFactory} with the given function that
     * creates the shared context object. Make sure to also call {@link
     * #withCreateServiceFn} that creates the service objects. You can use the
     * shared context as a shared service object as well, by returning it from
     * {@code createServiceFn}. To achieve this more conveniently, use {@link
     * ServiceFactories#sharedService} instead of this method. If you don't need
     * a shared context at all, just independent service instances, you can use
     * the convenience of {@link ServiceFactories#nonSharedService}.
     * <p>
     * <strong>Note:</strong> if your service has a blocking API (e.g., doing
     * synchronous IO or acquiring locks), you must call {@link
     * #toNonCooperative()} as a hint to the Jet execution engine to start a
     * dedicated thread for those calls. Failing to do this can cause severe
     * performance problems. You should also carefully consider how much local
     * parallelism you need for this step since each parallel tasklet needs its
     * own thread. Call {@link GeneralStage#setLocalParallelism
     * stage.setLocalParallelism()} to set an explicit level, otherwise it will
     * depend on the number of cores on the Jet machine, which makes no sense
     * for blocking code.
     *
     * @param createContextFn the function to create new context object, given a {@link
     *                        ProcessorSupplier.Context}. Called once per Jet member. It must be
     *                        stateless.
     * @param <C> type of the service context instance
     *
     * @return a new factory instance, not yet ready to use (needs the {@code
     *         createServiceFn})
     */
    @Nonnull
    public static <C> ServiceFactory<C, Void> withCreateContextFn(
            @Nonnull FunctionEx<? super ProcessorSupplier.Context, ? extends C> createContextFn
    ) {
        checkSerializable(createContextFn, "createContextFn");
        return new ServiceFactory<>(createContextFn);
    }

    /**
     * Returns a copy of this {@code ServiceFactory} with the {@code
     * destroyContext} function replaced with the given function.
     * <p>
     * Jet calls this function at the end of the job for each shared context
     * object it created (one on each cluster member).
     *
     * @param destroyContextFn the function to destroy the shared service
     *     context. It must be stateless.
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ServiceFactory<C, S> withDestroyContextFn(@Nonnull ConsumerEx<? super C> destroyContextFn) {
        checkSerializable(destroyContextFn, "destroyContextFn");
        ServiceFactory<C, S> copy = clone();
        copy.destroyContextFn = destroyContextFn;
        return copy;
    }

    /**
     * Returns a copy of this {@code ServiceFactory} with the given {@code
     * createService} function.
     * <p>
     * Jet calls this function to create each parallel instance of the service
     * object (their number on each cluster member is determined by {@link
     * GeneralStage#setLocalParallelism(int) stage.localParallelism}). Each
     * invocation gets the {@linkplain #createContextFn() shared context
     * instance} as the parameter, as well as the lower-level {@link
     * Processor.Context}.
     * <p>
     * Since the call of this method establishes the {@code <S>} type parameter
     * of the service factory, you must call it before setting the {@link
     * #withDestroyServiceFn(ConsumerEx) destroyService} function. Calling
     * this method resets any pre-existing {@code destroyService} function to a
     * no-op.
     *
     * @param createServiceFn the function that creates the service instance.
     *     It must be stateless.
     * @return a copy of this factory with the supplied create-service-function
     */
    @Nonnull
    public <S_NEW> ServiceFactory<C, S_NEW> withCreateServiceFn(
            @Nonnull BiFunctionEx<? super Processor.Context, ? super C, ? extends S_NEW> createServiceFn
    ) {
        checkSerializable(createServiceFn, "createServiceFn");
        @SuppressWarnings("unchecked")
        ServiceFactory<C, S_NEW> copy = (ServiceFactory<C, S_NEW>) clone();
        copy.createServiceFn = createServiceFn;
        return copy;
    }

    /**
     * Returns a copy of this {@code ServiceFactory} with the {@code
     * destroyService} function replaced with the given function.
     * <p>
     * The destroy function is called at the end of the job to destroy all
     * created services objects.
     *
     * @param destroyServiceFn the function to destroy the service instance.
     *                         This function is called once per processor instance. It must be
     *                         stateless.
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ServiceFactory<C, S> withDestroyServiceFn(@Nonnull ConsumerEx<? super S> destroyServiceFn) {
        checkSerializable(destroyServiceFn, "destroyServiceFn");
        ServiceFactory<C, S> copy = clone();
        copy.destroyServiceFn = destroyServiceFn;
        return copy;
    }

    /**
     * Returns a copy of this {@code ServiceFactory} with the {@code
     * isCooperative} flag set to {@code false}. Call this method if your
     * service doesn't follow the {@linkplain Processor#isCooperative()
     * cooperative processor contract}, that is if it waits for IO, blocks for
     * synchronization, takes too long to complete etc. If the service will
     * perform async operations, you can typically use a cooperative
     * processor. Cooperative processors offer higher performance.
     *
     * @return a copy of this factory with the {@code isCooperative} flag set
     * to {@code false}.
     */
    @Nonnull
    public ServiceFactory<C, S> toNonCooperative() {
        ServiceFactory<C, S> copy = clone();
        copy.isCooperative = false;
        return copy;
    }

    /**
     * Attaches a file to this service factory under the given ID. It will
     * become a part of the Jet job and available to {@link #createContextFn()}
     * as {@link ProcessorSupplier.Context#attachedFile
     * procSupplierContext.attachedFile(id)}.
     *
     * @return a copy of this factory with the file attached
     *
     * @since Jet 4.0
     */
    @Nonnull
    public ServiceFactory<C, S> withAttachedFile(@Nonnull String id, @Nonnull File file) {
        if (!file.isFile() || !file.canRead()) {
            throw new IllegalArgumentException("Not an existing, readable file: " + file);
        }
        return attachFileOrDir(id, file);
    }

    /**
     * Attaches a directory to this service factory under the given ID. It will
     * become a part of the Jet job and available to {@link #createContextFn()}
     * as {@link ProcessorSupplier.Context#attachedDirectory
     * procSupplierContext.attachedDirectory(id)}.
     *
     * @return a copy of this factory with the directory attached
     *
     * @since Jet 4.0
     */
    @Nonnull
    public ServiceFactory<C, S> withAttachedDirectory(@Nonnull String id, @Nonnull File directory) {
        if (!directory.isDirectory() || !directory.canRead()) {
            throw new IllegalArgumentException("Not an existing, readable directory: " + directory);
        }
        return attachFileOrDir(id, directory);
    }

    @Nonnull
    private ServiceFactory<C, S> attachFileOrDir(String id, @Nonnull File file) {
        ServiceFactory<C, S> copy = clone();
        copy.attachedFiles.put(id, file);
        return copy;
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with any attached files
     * removed.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public ServiceFactory<C, S> withoutAttachedFiles() {
        ServiceFactory<C, S> copy = clone();
        copy.attachedFiles = emptyMap();
        return copy;
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with setting the
     * required permission. This is en Enterprise feature.
     */
    @Nonnull
    public ServiceFactory<C, S> withPermission(@Nonnull Permission permission) {
        ServiceFactory<C, S> copy = clone();
        copy.permission = permission;
        return copy;
    }

    /**
     * Returns the function that creates the shared context object. Each
     * Jet member creates one such object and passes it to all the parallel
     * service instances.
     *
     * @see #withCreateContextFn(FunctionEx)
     */
    @Nonnull
    public FunctionEx<? super ProcessorSupplier.Context, ? extends C> createContextFn() {
        return createContextFn;
    }

    /**
     * Returns the function that creates the service object. There can be many
     * parallel service objects on each Jet member serving the same pipeline
     * stage, their number is determined by {@link
     * GeneralStage#setLocalParallelism(int) stage.localParallelism}.
     *
     * @see #withCreateServiceFn(BiFunctionEx)
     */
    @Nonnull
    public BiFunctionEx<? super Processor.Context, ? super C, ? extends S> createServiceFn() {
        return createServiceFn;
    }

    /**
     * Returns the function that destroys the service object at the end of the
     * Jet job.
     *
     * @see #withDestroyServiceFn(ConsumerEx)
     */
    @Nonnull
    public ConsumerEx<? super S> destroyServiceFn() {
        return destroyServiceFn;
    }

    /**
     * Returns the function that destroys the shared context object at the end
     * of the Jet job.
     *
     * @see #withDestroyContextFn(ConsumerEx)
     */
    @Nonnull
    public ConsumerEx<? super C> destroyContextFn() {
        return destroyContextFn;
    }

    /**
     * Returns the {@code isCooperative} flag, see {@link #toNonCooperative()}.
     */
    public boolean isCooperative() {
        return isCooperative;
    }

    /**
     * Returns the files and directories attached to this service factory. They
     * will become a part of the Jet job and available to {@link
     * #createContextFn()} as {@link ProcessorSupplier.Context#attachedFile
     * procSupplierContext.attachedFile(file.toString())} or
     * {@link ProcessorSupplier.Context#attachedDirectory
     * procSupplierContext.attachedDirectory(directory.toString())}.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public Map<String, File> attachedFiles() {
        return Collections.unmodifiableMap(attachedFiles);
    }

    /**
     * Returns the required permission to use this factory.
     * This is an Enterprise feature.
     */
    @Nullable
    public Permission permission() {
        return permission;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ServiceFactory<C, S> clone() {
        try {
            ServiceFactory<C, S> copy = (ServiceFactory<C, S>) super.clone();
            copy.attachedFiles = new HashMap<>(attachedFiles);
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new JetException(getClass() + " is not cloneable", e);
        }
    }
}
