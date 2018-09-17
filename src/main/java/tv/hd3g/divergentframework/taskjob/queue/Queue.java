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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import tv.hd3g.divergentframework.taskjob.worker.Engine;

/**
 * Connect Engines and Broker
 */
public interface Queue {
	
	public void registerEngine(Engine engine);
	
	public void unRegisterEngine(Engine engine);
	
	/**
	 * Don't get new Actions, wait the actual actions ends to stop.
	 * @return future for watch real stops
	 */
	public CompletableFuture<Void> prepareToStop(Executor executor);
	
	public boolean isRunning();
	
	public boolean isPendingStop();
	
	public List<String> getActualEnginesContextTypes(boolean only_with_free_workers);
	
}
