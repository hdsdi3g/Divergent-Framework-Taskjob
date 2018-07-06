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
package tv.hd3g.divergentframework.taskjob.worker;

import java.util.Arrays;
import java.util.function.Supplier;

import com.google.gson.Gson;

import tv.hd3g.divergentframework.taskjob.broker.Broker;
import tv.hd3g.divergentframework.taskjob.broker.Job;

public class GenericEngine<T> {
	
	private final Engine internal;
	private final Gson gson;
	private final Class<T> context_class;
	
	public GenericEngine(int max_worker_count, String base_thread_name, Gson gson, Class<T> context_class, Supplier<GenericWorker<T>> createWorker) {
		this.context_class = context_class;
		if (context_class == null) {
			throw new NullPointerException("\"context_class\" can't to be null");
		}
		
		internal = new Engine(max_worker_count, base_thread_name, Arrays.asList(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE + context_class.getName()), context_type -> {
			if (context_type.equals(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE + context_class.getName()) == false) {
				throw new RuntimeException("Can't manager this context: " + context_type);
			}
			return new InternalWorker(createWorker.get());
		});
		this.gson = gson;
		if (gson == null) {
			throw new NullPointerException("\"gson\" can't to be null");
		}
	}
	
	private class InternalWorker implements Worker {
		
		GenericWorker<T> engine;
		
		public InternalWorker(GenericWorker<T> engine) {
			this.engine = engine;
			if (engine == null) {
				throw new NullPointerException("\"engine\" can't to be null");
			}
		}
		
		public void process(Job referer, Broker broker, Supplier<Boolean> shouldStopProcessing) throws Throwable {
			engine.process(referer, gson.fromJson(referer.getContextContent(), context_class), broker, shouldStopProcessing);
		}
		
		public Runnable onStopProcessing() {
			return engine.onStopProcessing();
		}
		
	}
	
	public Engine getEngine() {
		return internal;
	}
}
