/*
    * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
    *
    * This code is free software; you can redistribute it and/or modify it
    * under the terms of the GNU General Public License version 2 only, as
    * published by the Free Software Foundation.  Oracle designates this
    * particular file as subject to the "Classpath" exception as provided
    * by Oracle in the LICENSE file that accompanied this code.
    *
    * This code is distributed in the hope that it will be useful, but WITHOUT
    * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
    * version 2 for more details (a copy is included in the LICENSE file that
    * accompanied this code).
    *
    * You should have received a copy of the GNU General Public License version
    * 2 along with this work; if not, write to the Free Software Foundation,
    * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
    *
    * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
    * or visit www.oracle.com if you need additional information or have any
    * questions.
    */

    /* This file is available under and governed by the GNU General Public
    * License version 2 only, as published by the Free Software Foundation.
    * However, the following notice accompanied the original version of this
    * file:
    *
    * Written by Doug Lea with assistance from members of JCP JSR-166
    * Expert Group and released to the public domain, as explained at
    * http://creativecommons.org/publicdomain/zero/1.0/
    */
package com.hazelcast.util.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.ExecutionCallback;

public class ExecutorCompletionService<V> implements CompletionService<V> {
	private final Executor executor;
	private final BlockingQueue<Future<V>> completionQueue;

	/**
	 * Creates an ExecutorCompletionService using the supplied executor for base
	 * task execution and a {@link LinkedBlockingQueue} as a completion queue.
	 * 
	 * @param executor
	 *            the executor to use
	 * @throws NullPointerException
	 *             if executor is <tt>null</tt>
	 */
	public ExecutorCompletionService(Executor executor) {
		if (executor == null)
			throw new NullPointerException();
		this.executor = executor;
		this.completionQueue = new LinkedBlockingQueue<Future<V>>();
	}

	/**
	 * Creates an ExecutorCompletionService using the supplied executor for base
	 * task execution and the supplied queue as its completion queue.
	 * 
	 * @param executor
	 *            the executor to use
	 * @param completionQueue
	 *            the queue to use as the completion queue normally one
	 *            dedicated for use by this service
	 * @throws NullPointerException
	 *             if executor or completionQueue are <tt>null</tt>
	 */
	public ExecutorCompletionService(Executor executor, BlockingQueue<Future<V>> completionQueue) {
		if (executor == null || completionQueue == null)
			throw new NullPointerException();
		this.executor = executor;
		this.completionQueue = completionQueue;
	}

	public Future<V> submit(Callable<V> task) {
		if (task == null)
			throw new NullPointerException();
		RunnableFuture<V> f = newTaskFor(task);
		executor.execute(f);
		return f;
	}

	public Future<V> submit(Runnable task, V result) {
		if (task == null)
			throw new NullPointerException();
		RunnableFuture<V> f = newTaskFor(task, result);
		executor.execute(f);
		return f;
	}

	public Future<V> take() throws InterruptedException {
		return completionQueue.take();
	}

	public Future<V> poll() {
		return completionQueue.poll();
	}

	public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
		return completionQueue.poll(timeout, unit);
	}
	
	private ExecutionCallback<V> addItselfInCompletionQueue() {
		return new ExecutionCallback<V>() {
			public void done(Future<V> future) {
				completionQueue.add(future);
			}
		};
	}
	
	private RunnableFuture<V> newTaskFor(Callable<V> task) {
		DistributedTask<V> f = new DistributedTask<V>(task);
		f.setExecutionCallback(addItselfInCompletionQueue());
		return f;
	}

	private RunnableFuture<V> newTaskFor(Runnable task, V result) {
		DistributedTask<V> f = new DistributedTask<V>(task, result);
		f.setExecutionCallback(addItselfInCompletionQueue());
		return f;
	}

}
