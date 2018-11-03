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
package tv.hd3g.divergentframework.taskjob.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import tv.hd3g.divergentframework.taskjob.broker.Broker;
import tv.hd3g.divergentframework.taskjob.events.EngineEventObserver;
import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

/**
 * Connect Engines and Broker, in this Java process.
 */
public class LocalQueue implements Queue {
	private static final Logger log = LogManager.getLogger();
	
	private final Broker broker;
	
	private final List<Engine> engines;
	private volatile boolean pending_stop;
	
	private final ThreadPoolExecutor maintenance_pool;
	
	private final ArrayList<EngineEventObserver> engine_observer_list;
	
	private final InternalDispatcherEngineEventObserver engine_observer;
	
	public LocalQueue(Broker broker) {
		engine_observer_list = new ArrayList<>();
		engine_observer = new InternalDispatcherEngineEventObserver();
		
		this.broker = broker;
		if (broker == null) {
			throw new NullPointerException("\"broker\" can't to be null");
		}
		maintenance_pool = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
			Thread t = new Thread(r);
			t.setDaemon(true);
			t.setPriority(Thread.MIN_PRIORITY);
			t.setName("Maintenance " + new Date());
			return t;
		});
		
		engines = new ArrayList<>();
		pending_stop = false;
		
		broker.registerCallbackOnNewLocalJobsActivity(() -> {
			if (isPendingStop()) {
				return;
			}
			
			log.trace("Callback queue for some new action");
			searchAndStartNewActions();
		});
	}
	
	private class InternalDispatcherEngineEventObserver implements EngineEventObserver {
		public void onEngineChangeContextRequirementTags(Engine engine) {
			engine_observer_list.parallelStream().forEach(o -> {
				o.onEngineChangeContextRequirementTags(engine);
			});
		}
		
		public void onEngineEndsProcess(Engine engine, WorkerThread w_t) {
			engine_observer_list.parallelStream().forEach(o -> {
				o.onEngineEndsProcess(engine, w_t);
			});
		}
		
		public void onEngineStartProcess(Engine engine, WorkerThread w_t) {
			engine_observer_list.parallelStream().forEach(o -> {
				o.onEngineStartProcess(engine, w_t);
			});
		}
		
		public void onEngineStop(Engine engine) {
			engine_observer_list.parallelStream().forEach(o -> {
				o.onEngineStop(engine);
			});
		}
		
		public void onRegisterEngine(Engine engine) {
			engine_observer_list.parallelStream().forEach(o -> {
				o.onRegisterEngine(engine);
			});
		}
		
		public void onUnRegisterEngine(Engine engine) {
			engine_observer_list.parallelStream().forEach(o -> {
				o.onUnRegisterEngine(engine);
			});
		}
	}
	
	/**
	 * @return this
	 */
	public synchronized LocalQueue addEngineObserver(EngineEventObserver engine_observer) {
		if (engine_observer == null) {
			throw new NullPointerException("\"engine_observer\" can't to be null");
		}
		
		engine_observer_list.add(engine_observer);
		return this;
	}
	
	public List<Engine> getEnginesByContextType(String context_name) {
		return engines.stream().filter(engine -> {
			return engine.getAllHandledContextTypes().contains(context_name);
		}).collect(Collectors.toList());
	}
	
	/**
	 * Engines can set anytime the same context_type.
	 */
	public void registerEngine(Engine engine) {
		if (isPendingStop()) {
			throw new RuntimeException("Can't add engine " + engine + " for a stopped queue");
		}
		
		synchronized (engines) {
			if (engines.contains(engine)) {
				return;
			}
			engines.add(engine);
			
			if (engine_observer != null) {
				engine_observer.onRegisterEngine(engine);
			}
		}
		
		engine.setObserver(engine_observer);
		
		searchAndStartNewActions();
	}
	
	public void unRegisterEngine(Engine engine) {
		synchronized (engines) {
			engines.remove(engine);
			
			if (engine_observer != null) {
				engine_observer.onUnRegisterEngine(engine);
			}
		}
	}
	
	public boolean isPendingStop() {
		return pending_stop;
	}
	
	public boolean isRunning() {
		return maintenance_pool.getActiveCount() > 0 | engines.stream().anyMatch(engine -> {
			return engine.isRunning();
		});
	}
	
	public CompletableFuture<Void> prepareToStop(Executor executor) {
		pending_stop = true;
		
		synchronized (engines) {
			engines.forEach(engine -> {
				engine.stopCurrentAll();
			});
			
			List<CompletableFuture<Void>> cf = engines.stream().map(engine -> {
				return engine.stopCurrentAll(executor);
			}).collect(Collectors.toList());
			
			return CompletableFuture.allOf(cf.toArray(new CompletableFuture[cf.size()]));
		}
	}
	
	/**
	 * @param only_with_free_workers set false for all engines
	 */
	public List<String> getActualEnginesContextTypes(boolean only_with_free_workers) {
		if (isPendingStop()) {
			return Collections.emptyList();
		}
		
		return engines.stream().filter(engine -> {
			if (only_with_free_workers) {
				return engine.actualFreeWorkers() > 0;
			} else {
				return true;
			}
		}).flatMap(engine -> {
			return engine.getAllHandledContextTypes().stream();
		}).distinct().collect(Collectors.toList());
	}
	
	private static final Predicate<Engine> engineHasFreeWorkers = engine -> engine.actualFreeWorkers() > 0;
	
	public void searchAndStartNewActions() {
		if (isPendingStop()) {
			return;
		}
		
		broker.getNextJobs(getActualEnginesContextTypes(true), () -> {
			return engines.stream().mapToInt(engine -> {
				return engine.actualFreeWorkers();
			}).sum();
		}, (context_type, context_r_tags) -> {
			return engines.stream().filter(engine -> {
				return engine.getAllHandledContextTypes().contains(context_type);
			}).filter(engineHasFreeWorkers).allMatch(engine -> {
				return engine.getContextRequirementTags().containsAll(context_r_tags);
			});
		}, selected_action -> {
			Optional<Engine> o_engine_potentially_free = engines.stream().filter(engine -> {
				return engine.getAllHandledContextTypes().contains(selected_action.getContextType());
			}).filter(engineHasFreeWorkers).filter(engine -> {
				return engine.getContextRequirementTags().containsAll(selected_action.getContextRequirementTags());
			}).findFirst();
			
			if (o_engine_potentially_free.isPresent() == false) {
				/**
				 * Too late, no free workers.
				 */
				return false;
			}
			
			return o_engine_potentially_free.get().addProcess(selected_action, broker, () -> {
				/**
				 * onAfterProcess
				 */
				if (isPendingStop()) {
					return;
				}
				
				log.trace("Queue a searchAndStartNewActions");
				maintenance_pool.execute(() -> {
					searchAndStartNewActions();
				});
			});
		});
	}
	
}
