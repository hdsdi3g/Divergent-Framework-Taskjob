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
package tv.hd3g.divergentframework.taskjob.gui;

import java.util.stream.Collectors;

import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

class TableItemEngineWorker {
	Engine engine;
	WorkerThread worker;
	
	String state = "";
	String context_type = "";
	String context_requirement_tags = "";
	String ref = "";
	String job_key = "";
	String descr = "";
	
	void updateEngineOnly() {
		state = String.valueOf(engine.maxWorkersCount() - engine.actualFreeWorkers()) + "/" + String.valueOf(engine.maxWorkersCount());
		context_type = engine.getAllHandledContextTypes().stream().collect(Collectors.joining(", "));
		context_requirement_tags = engine.getContextRequirementTags().stream().collect(Collectors.joining(", "));
		ref = engine.getKey().toString();
	}
	
	void updateWorkerOnly() {
		context_type = worker.getJob().getContextType();
		context_requirement_tags = worker.getJob().getContextRequirementTags().stream().collect(Collectors.joining(", "));
		job_key = worker.getJob().getKey().toString();
		descr = worker.getJob().getDescription();
	}
	
}
