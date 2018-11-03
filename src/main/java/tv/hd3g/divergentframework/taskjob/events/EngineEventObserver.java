/*
 * This file is part of DivergentFameworkTaskjob.
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
package tv.hd3g.divergentframework.taskjob.events;

import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

public interface EngineEventObserver {
	
	default void onEngineChangeContextRequirementTags(Engine engine) {
	}
	
	default void onEngineStop(Engine engine) {
	}
	
	default void onEngineStartProcess(Engine engine, WorkerThread w_t) {
	}
	
	default void onEngineEndsProcess(Engine engine, WorkerThread w_t) {
	}
	
	default void onRegisterEngine(Engine engine) {
	}
	
	default void onUnRegisterEngine(Engine engine) {
	}
	
}
