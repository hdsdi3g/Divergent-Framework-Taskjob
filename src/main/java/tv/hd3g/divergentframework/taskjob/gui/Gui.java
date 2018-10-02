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

import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;
import tv.hd3g.divergentframework.taskjob.worker.Engine;

public class Gui extends Application {
	private static Logger log = LogManager.getLogger();
	
	private GuiController controller;
	private InMemoryLocalTaskJob task_job;
	
	public void start(Stage primary_stage) throws Exception {
		log.info("Start JavaFX GUI Interface");
		FXMLLoader d = new FXMLLoader();
		d.setResources(ResourceBundle.getBundle(getClass().getPackage().getName() + ".messages"));
		BorderPane root = (BorderPane) d.load(getClass().getResource("GuiPanel.fxml").openStream());
		
		controller = d.getController();
		
		task_job = new InMemoryLocalTaskJob(1000, 8, 8, 8, TimeUnit.SECONDS);// TODO3 externalize this
		controller.startApp(primary_stage, root, () -> {
			try {
				task_job.prepareToStop(r -> r.run()).get();
			} catch (Exception e) {
				log.error("Can't stop task_jobs", e);
			}
			System.exit(0);
		});
		
		controller.linkToTaskJob(task_job);
		
		Thread t_demo = new Thread(() -> {
			try {
				Job job1 = task_job.createJob("Descr", "EXT", "context", new JsonObject(), List.of("RqT1", "RqT2"));// TODO2 display context content json
				Job job1_sub = task_job.addSubJob(job1, "Sub job 1", "ref2", "ctx2", new JsonObject(), List.of("RqT3", "RqT4"));
				
				Thread.sleep(500);
				JsonObject jp_sj2 = new JsonObject();
				jp_sj2.addProperty("boolean", true);
				jp_sj2.add("nulle", JsonNull.INSTANCE);
				jp_sj2.addProperty("integer", 42);
				jp_sj2.addProperty("floated", 4.2);
				JsonArray ja = new JsonArray();
				ja.add("one");
				ja.add("two");
				ja.add("tree");
				jp_sj2.add("ja", ja);
				
				task_job.createJob("Descr2", "EXT2", "context2", new JsonObject(), List.of("RqT1", "RqT2"));
				task_job.switchStatus(job1_sub, TaskStatus.CANCELED);
				task_job.addSubJob(job1_sub, "Sub job 2", "ref2", "ctx2", jp_sj2, List.of("RqT5", "RqT6"));
				
				Thread.sleep(500);
				task_job.switchStatus(job1, TaskStatus.POSTPONED);
				task_job.switchStatus(job1_sub, TaskStatus.POSTPONED);
			} catch (InterruptedException e) {
			}
		});
		t_demo.start();
		
		Thread t_demo2 = new Thread(() -> {
			
			Engine engine = new Engine(4, "TstEng", List.of("type1", "type2"), ctx_type -> {
				return (job, broker, shouldStopProcessing) -> {
					/**
					 * Do nothing, just a test.
					 */
				};
			});
			task_job.registerEngine(engine);
			engine.setContextRequirementTags(List.of("RQ1", "RQ2"));
			
			Engine engine2 = new Engine(1, "TstEng2", List.of("context2"), ctx_type -> {
				return (job, broker, shouldStopProcessing) -> {
					/**
					 * Simulate an exec
					 */
					log.info("START JOB");
					IntStream.range(0, 10_000).forEach(i -> {
						broker.updateProgression(job, i, 10_000);
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
						}
					});
					log.info("END JOB");// FIXME flush before ends
				};
			});
			task_job.registerEngine(engine2);
			engine2.setContextRequirementTags(List.of("RqT1", "RqT2"));
		});
		t_demo2.start();
		
		// TODO, Action global: task_job.checkStoreConsistency();
		// TODO, Action global: task_job.prepareToStop(executor)
		// TODO, Action global: stop all actual engines (to do...) and destroy all engines.
		// TODO, Action for selected job: task_job.switchStatus(job, new_status);
		// TODO, Action for selected engine: task_job.unRegisterEngine(engine);
		// TODO, Action for selected engine: engine.stopCurrentAll(executor)
		// TODO, Action for selected worker_thread: worker.waitToStop(Executor executor)
		
		// TODO3 create Job log ? with a specific and contextual logger ?
	}
	
	public static void main(String[] args) {
		launch(args);
	}
	
	public void stop() {
		log.info("JavaFX GUI Interface is stop");
	}
	
}
