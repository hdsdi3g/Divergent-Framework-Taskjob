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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.MapChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.ObservableMap;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.TreeItem;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.worker.Engine;

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
		
		ObservableList<Engine> engines = FXCollections.observableArrayList();
		ObservableMap<UUID, Job> jobs = FXCollections.observableHashMap();
		
		task_jobs = new InMemoryLocalTaskJob(1000, 8, 8, 8, TimeUnit.SECONDS, jobs, engines);// XXX externalize this
		controller.startApp(primary_stage, root);
		
		final ObservableList<TreeItem<Job>> table_jobs_content = controller.getTableJobContent();
		
		jobs.addListener((MapChangeListener<? super UUID, ? super Job>) change -> {
			if (change.wasAdded()) {
				table_jobs_content.add(new TreeItem<Job>(change.getValueAdded()));
			} else if (change.wasRemoved()) {
				table_jobs_content.removeAll(table_jobs_content.filtered(ti_j -> ti_j.getValue().getKey().equals(change.getKey())));
			} else {
				throw new RuntimeException("What change ? " + change.getKey());
			}
		});
		
		/*controller.get ItemsList().addListener((ListChangeListener<? super TreeItem< >>) change -> {
		// TODO same with engines
		});*/
		
		new GuiEventDispatcher(controller, task_jobs);
		
		Thread t = new Thread(() -> {
			task_jobs.createJob("Descr", "EXT", "context", new JsonObject(), List.of("RqT1", "RqT2"));
			/*try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			task_jobs.createJob("Descr2", "EXT2", "context2", new JsonObject(), List.of("RqT1", "RqT2"));*/
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
