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

import java.util.function.Supplier;

import tv.hd3g.taskjob.broker.Broker;
import tv.hd3g.taskjob.broker.Job;

/**
 * Execute Action, used only once.
 */
@FunctionalInterface
public interface Worker {
	
	/**
	 * @param referer, job to process
	 * @param broker, broker to use for manipulate job status
	 * @param shouldStopProcessing if should stop processing now
	 */
	public void process(Job referer, Broker broker, Supplier<Boolean> shouldStopProcessing) throws Throwable;
	
	/**
	 * Will be started if a manual stop is triggered.
	 */
	public default Runnable onStopProcessing() {
		return () -> {
		};
	}
	
}
