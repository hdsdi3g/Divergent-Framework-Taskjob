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
package tv.hd3g.divergentframework.taskjob.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import tv.hd3g.divergentframework.taskjob.broker.Broker;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;
import tv.hd3g.divergentframework.taskjob.events.EngineEventObserver;

/**
 * Factory for create specific workers on demand.
 */
public final class Engine {
	private static final Logger log = LogManager.getLogger();
	
	private final UUID key;
	private final LinkedBlockingQueue<WorkerThread> runnables;
	private final String base_thread_name;
	private final List<String> all_handled_context_types;
	private final Function<String, Worker> createWorkerByContextType;
	private final ArrayList<String> context_requirement_tags;
	private final int max_worker_count;
	
	private EngineEventObserver observer;
	
	/**
	 * @param createWorkerByContextType context_type -> Worker ; Worker: ctx_type -> return (job, broker, shouldStopProcessing)
	 */
	public Engine(int max_worker_count, String base_thread_name, List<String> all_handled_context_types, Function<String, Worker> createWorkerByContextType) {
		key = UUID.randomUUID();
		runnables = new LinkedBlockingQueue<>(max_worker_count);
		if (max_worker_count == 0) {
			throw new IndexOutOfBoundsException("\"max_worker_count\" can't to be 0");
		}
		this.max_worker_count = max_worker_count;
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
	
	public synchronized void setObserver(EngineEventObserver observer) {
		this.observer = observer;
	}
	
	public String toString() {
		return "Engine \"" + base_thread_name + "\" (Wkrs " + runnables.size() + "/" + max_worker_count + ") for " + all_handled_context_types.size() + " context(s)";
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
		
		if (observer != null) {
			observer.onEngineChangeContextRequirementTags(this);
		}
		
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
		if (observer != null) {
			observer.onEngineStop(this);
		}
		
		runnables.stream().filter(t -> {
			return t.isAlive();
		}).forEach(t -> {
			t.wantToStop();
		});
	}
	
	public CompletableFuture<Void> stopCurrentAll(Executor executor) {
		if (observer != null) {
			observer.onEngineStop(this);
		}
		
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
	
	public int maxWorkersCount() {
		return max_worker_count;
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
			if (observer != null) {
				observer.onEngineEndsProcess(this, w_t);
			}
			
			log.trace("Remove item " + w_t + " from runnables list");
			runnables.remove(w_t);
			
			onAfterProcess.run();
		});
		
		log.trace("Start worker " + w_t);
		w_t.start();
		
		if (observer != null) {
			observer.onEngineStartProcess(this, w_t);
		}
		
		return true;
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (key == null ? 0 : key.hashCode());
		return result;
	}
	
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Engine other = (Engine) obj;
		if (key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!key.equals(other.key)) {
			return false;
		}
		return true;
	}
	
}
