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
package tv.hd3g.taskjob.queue;

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

import org.apache.log4j.Logger;

import tv.hd3g.taskjob.broker.Broker;
import tv.hd3g.taskjob.worker.Engine;

public class LiveQueue implements Queue {
	private static Logger log = Logger.getLogger(LiveQueue.class);
	
	private final Broker broker;
	
	private final ArrayList<Engine> engines;
	private volatile boolean pending_stop;
	
	private final ThreadPoolExecutor maintenance_pool;
	
	public LiveQueue(Broker broker, String base_thread_name) {
		this.broker = broker;
		if (broker == null) {
			throw new NullPointerException("\"broker\" can't to be null");
		}
		maintenance_pool = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
			Thread t = new Thread(r);
			t.setDaemon(true);
			t.setPriority(Thread.MIN_PRIORITY);
			t.setName("Maintenance Thread " + new Date());
			return t;
		});
		
		engines = new ArrayList<>();
		pending_stop = false;
		
		broker.onNewLocalJobsActivity(() -> {
			if (isPendingStop()) {
				return;
			}
			
			log.trace("Callback queue for some new action");
			searchAndStartNewActions();
		});
	}
	
	public void registerEngine(Engine engine, int worker_count) {
		if (isPendingStop()) {
			throw new RuntimeException("Can't add engine " + engine + " for a stopped queue");
		}
		
		synchronized (engines) {
			if (engines.contains(engine)) {
				throw new RuntimeException("Can't add engine " + engine + ", it was previousely added");
			}
			
			List<String> previouly_added_handled_context_types = engines.stream().flatMap(eng -> {
				return eng.getAllHandledContextTypes().stream();
			}).filter(previouly_added_c_type -> {
				return engine.getAllHandledContextTypes().contains(previouly_added_c_type);
			}).collect(Collectors.toList());
			
			if (previouly_added_handled_context_types.isEmpty() == false) {
				throw new RuntimeException("Can't add engine " + engine + ", some handled_context_types was previouly added: " + previouly_added_handled_context_types);
			}
			
			engines.add(engine);
		}
		
		searchAndStartNewActions();
	}
	
	public void unRegisterEngine(Engine engine) {
		synchronized (engines) {
			engines.remove(engine);
		}
	}
	
	public boolean isPendingStop() {
		return pending_stop;
	}
	
	public boolean isStopped() {
		return engines.stream().noneMatch(engine -> {
			return engine.isRunning();
		});
	}
	
	public CompletableFuture<Void> prepareToStop(Executor executor) {
		pending_stop = true;
		
		synchronized (engines) {
			engines.forEach(engine -> {
				engine.stopAll();
			});
			
			List<CompletableFuture<Void>> cf = engines.stream().map(engine -> {
				return engine.stopAll(executor);
			}).collect(Collectors.toList());
			
			return CompletableFuture.allOf(cf.toArray(new CompletableFuture[cf.size()]));
		}
	}
	
	public List<String> getActualEnginesContextTypes() {
		if (isPendingStop()) {
			return Collections.emptyList();
		}
		
		return engines.stream().filter(engine -> {
			return engine.actualFreeWorkers() > 0;
		}).flatMap(engine -> {
			return engine.getAllHandledContextTypes().stream();
		}).distinct().collect(Collectors.toList());
	}
	
	private static final Predicate<Engine> engineHasFreeWorkers = engine -> engine.actualFreeWorkers() > 0;
	
	private void searchAndStartNewActions() {
		if (isPendingStop()) {
			return;
		}
		
		Runnable onAfterProcess = () -> {
			if (isPendingStop()) {
				return;
			}
			
			log.trace("Queue a searchAndStartNewActions");
			maintenance_pool.execute(() -> {
				searchAndStartNewActions();
			});
		};
		
		broker.getNextActions(getActualEnginesContextTypes(), (context_type, context_r_tags) -> {
			return engines.stream().filter(engine -> {
				return engine.getAllHandledContextTypes().contains(context_type);
			}).filter(engineHasFreeWorkers).allMatch(engine -> {
				return context_r_tags.containsAll(engine.getContextRequirementTags());
			});
		}, selected_action -> {
			Optional<Engine> o_engine_potentially_free = engines.stream().filter(engine -> {
				return engine.getAllHandledContextTypes().contains(selected_action.getContextType());
			}).filter(engineHasFreeWorkers).filter(engine -> {
				return selected_action.getContextRequirementTags().containsAll(engine.getContextRequirementTags());
			}).findFirst();
			
			if (o_engine_potentially_free.isPresent() == false) {
				/**
				 * Too late, no free workers.
				 */
				return false;
			}
			
			return o_engine_potentially_free.get().addProcess(selected_action, broker, onAfterProcess);
		});
	}
	
}
