/*
 * This file is part of Divergent-Framework-Taskjob.
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
package tv.hd3g.taskjob.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import tv.hd3g.taskjob.broker.Broker;
import tv.hd3g.taskjob.broker.Job;
import tv.hd3g.taskjob.broker.TaskStatus;

/**
 * Factory for create specific workers on demand.
 */
public final class Engine {
	private static Logger log = Logger.getLogger(Engine.class);
	
	private final LinkedBlockingQueue<WorkerThread> runnables;
	private final String base_thread_name;
	private final List<String> all_handled_context_types;
	private final Function<String, Worker> createWorkerByContextType;
	private final ArrayList<String> context_requirement_tags;
	
	public Engine(int max_worker_count, String base_thread_name, List<String> all_handled_context_types, Function<String, Worker> createWorkerByContextType) {
		runnables = new LinkedBlockingQueue<>(max_worker_count);
		if (max_worker_count == 0) {
			throw new IndexOutOfBoundsException("\"max_worker_count\" can't to be 0");
		}
		this.all_handled_context_types = all_handled_context_types;
		if (all_handled_context_types == null) {
			throw new NullPointerException("\"all_handled_context_types\" can't to be null");
		} else if (all_handled_context_types.isEmpty()) {
			throw new IndexOutOfBoundsException("\"all_handled_context_types\" can't to be empty");
		}
		this.base_thread_name = base_thread_name;
		if (base_thread_name == null) {
			throw new NullPointerException("\"base_thread_name\" can't to be null");
		}
		this.createWorkerByContextType = createWorkerByContextType;
		if (createWorkerByContextType == null) {
			throw new NullPointerException("\"createWorkerByContextType\" can't to be null");
		}
		context_requirement_tags = new ArrayList<>();
	}
	
	public String toString() {
		int current = runnables.size() - runnables.remainingCapacity();
		return "Engine \"" + base_thread_name + "\" " + current + "/" + runnables.size() + " for " + all_handled_context_types;
	}
	
	/**
	 * @return this
	 */
	public Engine setContextRequirementTags(List<String> tags) {
		if (tags == null) {
			throw new NullPointerException("\"tags\" can't to be null");
		}
		context_requirement_tags.clear();
		context_requirement_tags.addAll(tags);
		return this;
	}
	
	public List<String> getContextRequirementTags() {
		return context_requirement_tags;
	}
	
	public List<String> getAllHandledContextTypes() {
		return all_handled_context_types;
	}
	
	/**
	 * non-blocking,
	 */
	public void stopCurrentAll() {
		runnables.stream().filter(t -> {
			return t.isAlive();
		}).forEach(t -> {
			t.wantToStop();
		});
	}
	
	public CompletableFuture<Void> stopCurrentAll(Executor executor) {
		List<CompletableFuture<Void>> cf = runnables.stream().filter(t -> {
			return t.isAlive();
		}).map(t -> {
			return t.waitToStop(executor);
		}).collect(Collectors.toList());
		
		if (cf.isEmpty()) {
			/**
			 * Do nothing.
			 */
			return CompletableFuture.runAsync(() -> System.currentTimeMillis());
		}
		
		return CompletableFuture.allOf(cf.toArray(new CompletableFuture[cf.size()]));
	}
	
	/**
	 * @return if some process are actually alive.
	 */
	public boolean isRunning() {
		return runnables.stream().anyMatch(t -> {
			return t.isAlive();
		});
	}
	
	public int actualFreeWorkers() {
		return runnables.remainingCapacity();
	}
	
	private final AtomicInteger created_thread_count = new AtomicInteger(0);
	
	/**
	 * @return true if it will start job, or false if it will be ignored
	 */
	public boolean addProcess(Job job, Broker broker, Runnable onAfterProcess) {
		if (all_handled_context_types.contains(job.getContextType()) == false) {
			throw new RuntimeException("Stupid queue: you don't check context_type before send this job to me. My all_handled_context_types: " + all_handled_context_types + ", job: " + job);
		} else if (context_requirement_tags.containsAll(job.getContextRequirementTags()) == false) {
			throw new RuntimeException("Stupid queue: you don't check context_requirement_tags before send this job to me. My context_requirement_tags: " + context_requirement_tags + ", job: " + job);
		}
		
		WorkerThread w_t = new WorkerThread(base_thread_name + "_" + created_thread_count.getAndIncrement(), job, broker, createWorkerByContextType.apply(job.getContextType()));
		try {
			if (runnables.offer(w_t) == false) {
				runnables.removeIf(t -> {
					return t.isAlive() == false;
				});
				
				if (runnables.offer(w_t, 10, TimeUnit.MILLISECONDS) == false) {
					return false;
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Can't wait", e);
		}
		
		broker.switchStatus(job, TaskStatus.PREPARING);
		
		w_t.setAfterProcess(() -> {
			log.trace("Remove item " + w_t + " from runnables list");
			runnables.remove(w_t);
			
			onAfterProcess.run();
		});
		
		log.trace("Start worker " + w_t);
		w_t.start();
		
		return true;
	}
	
}
