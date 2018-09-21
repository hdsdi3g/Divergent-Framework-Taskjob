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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javafx.application.Platform;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.events.EngineEventObserver;
import tv.hd3g.divergentframework.taskjob.events.JobEventObserver;
import tv.hd3g.divergentframework.taskjob.worker.Engine;

public class GuiEventDispatcher implements EngineEventObserver, JobEventObserver {
	
	private static Logger log = LogManager.getLogger();
	
	private final GuiController controller;
	private final ExecutorService executor;
	
	GuiEventDispatcher(GuiController controller, InMemoryLocalTaskJob task_job) {
		this.controller = controller;
		executor = Executors.newFixedThreadPool(1);
		task_job.setEngineObserver(this);
		task_job.setJobObserver(this);
	}
	
	/**
	 * Non blocking for Engine/Event, and polite from JajaFX side.
	 */
	private void exec(Runnable r) {
		executor.execute(() -> {
			Platform.runLater(r);
		});
	}
	
	public void onJobAfterInit(Job job) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onJobUpdate(Job job, JobUpdateSubject cause) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onJobUpdateProgression(Job job) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onJobAddSubJob(Job job, Job sub_job) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onEngineChangeContextRequirementTags(Engine engine) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onEngineStop(Engine engine) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onEngineStartProcess(Engine engine, Job job) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
	
	public void onEngineEndsProcess(Engine engine, Job job) {
		exec(() -> {
			// TODO Auto-generated method stub
		});
	}
}
