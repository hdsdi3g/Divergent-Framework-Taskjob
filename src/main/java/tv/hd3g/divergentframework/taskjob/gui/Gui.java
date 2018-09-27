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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;

public class Gui extends Application {
	private static Logger log = LogManager.getLogger();
	
	private GuiController controller;
	private InMemoryLocalTaskJob task_jobs;
	
	public void start(Stage primary_stage) throws Exception {
		log.info("Start JavaFX GUI Interface");
		FXMLLoader d = new FXMLLoader();
		d.setResources(ResourceBundle.getBundle(getClass().getPackage().getName() + ".messages"));
		BorderPane root = (BorderPane) d.load(getClass().getResource("GuiPanel.fxml").openStream());
		
		controller = d.getController();
		
		task_jobs = new InMemoryLocalTaskJob(1000, 8, 8, 8, TimeUnit.SECONDS);// TODO3 externalize this
		controller.startApp(primary_stage, root);
		
		new GuiEventDispatcher(controller, task_jobs);
		
		Thread t = new Thread(() -> {
			try {
				Job job1 = task_jobs.createJob("Descr", "EXT", "context", new JsonObject(), List.of("RqT1", "RqT2"));
				Thread.sleep(1000);
				task_jobs.createJob("Descr2", "EXT2", "context2", new JsonObject(), List.of("RqT1", "RqT2"));
				Thread.sleep(1000);
				task_jobs.switchStatus(job1, TaskStatus.POSTPONED);
				
			} catch (InterruptedException e) {
			}
		});
		t.start();
	}
	
	public static void main(String[] args) {
		launch(args);
	}
	
	public void stop() {
		log.info("JavaFX GUI Interface is stop");
	}
	
}
