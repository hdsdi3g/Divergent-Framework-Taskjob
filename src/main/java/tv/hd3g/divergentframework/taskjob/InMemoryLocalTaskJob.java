/*
 * This file is part of Divergent Framework Taskjob.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * Copyright (C) hdsdi3g for hd3g.tv 2018
 * 
*/
package tv.hd3g.divergentframework.taskjob;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import tv.hd3g.divergentframework.taskjob.broker.InMemoryBroker;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.events.EngineEventObserver;
import tv.hd3g.divergentframework.taskjob.queue.LocalQueue;
import tv.hd3g.divergentframework.taskjob.queue.Queue;
import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.GenericEngine;
import tv.hd3g.divergentframework.taskjob.worker.GenericWorker;

/**
 * Connect LocalQueue and InMemoryBroker for a full Taskjob utility.
 */
public class InMemoryLocalTaskJob extends InMemoryBroker implements Queue {
	private static final Logger log = LogManager.getLogger();
	
	private final LocalQueue queue;
	
	public InMemoryLocalTaskJob(int max_job_count, long abandoned_jobs_retention_time, long done_jobs_retention_time, long error_jobs_retention_time, TimeUnit unit) {
		super(max_job_count, abandoned_jobs_retention_time, done_jobs_retention_time, error_jobs_retention_time, unit);
		queue = new LocalQueue(this);
	}
	
	// TODO2 export checkStoreConsistency() to a callback system
	
	/**
	 * @return this
	 */
	public InMemoryLocalTaskJob setEngineObserver(EngineEventObserver engine_observer) {
		queue.setEngineObserver(engine_observer);
		return this;
	}
	
	public void registerEngine(Engine engine) {
		log.info("Register " + engine);
		queue.registerEngine(engine);
	}
	
	public void unRegisterEngine(Engine engine) {
		log.info("Unregister " + engine);
		queue.unRegisterEngine(engine);
	}
	
	public <T> void registerGenericEngine(int max_worker_count, String base_thread_name, Gson gson, Class<T> context_class, Supplier<GenericWorker<T>> createWorker) {
		GenericEngine<T> g_e = new GenericEngine<>(max_worker_count, base_thread_name, gson, context_class, createWorker);
		
		log.info("Register " + g_e.getEngine());
		queue.registerEngine(g_e.getEngine());
	}
	
	public <T> void unRegisterGenericEngine(Class<?> context_class) {
		log.info("Unregister " + context_class.getName());
		
		queue.getEnginesByContextType(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE + context_class.getName()).stream().distinct().forEach(engine -> {
			queue.registerEngine(engine);
		});
	}
	
	public CompletableFuture<Void> prepareToStop(Executor executor) {
		cancelCleanUpTask();
		return queue.prepareToStop(executor);
	}
	
	public boolean isRunning() {
		return queue.isRunning();
	}
	
	public boolean isPendingStop() {
		return queue.isPendingStop();
	}
	
	public List<String> getActualEnginesContextTypes(boolean only_with_free_workers) {
		return queue.getActualEnginesContextTypes(only_with_free_workers);
	}
	
}
