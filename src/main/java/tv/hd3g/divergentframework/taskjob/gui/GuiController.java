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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableView;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

public class GuiController {
	private static Logger log = LogManager.getLogger();
	
	private Stage stage;
	private BorderPane root;
	
	private final static SimpleDateFormat date_format = new SimpleDateFormat("HH:mm:ss");
	
	void startApp(Stage stage, BorderPane root) {
		this.stage = stage;
		this.root = root;
		
		Scene scene = new Scene(root);
		// scene.getStylesheets().add(getClass().getResource("application.css").toExternalForm());
		// stage.getIcons().add(new Image(getClass().getResourceAsStream("icon.png")));
		
		stage.setScene(scene);
		stage.show();
		
		stage.setOnCloseRequest(event -> {
			// TODO2 stop taskjob
			System.exit(0);
		});
		
		btnclose.setOnAction(event -> {
			// TODO2 stop taskjob
			event.consume();
			System.exit(0);
		});
		
		table_job_col_name.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getDescription());
		});
		
		table_job_col_type.setCellValueFactory(p -> {
			String ctx_type = p.getValue().getValue().getContextType();
			if (ctx_type.startsWith(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE)) {
				return new ReadOnlyStringWrapper(ctx_type.substring(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE.length()));
			} else {
				return new ReadOnlyStringWrapper(ctx_type + " (not java)");
			}
		});
		table_job_col_status.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getStatus().name());
		});
		table_job_col_progress.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getProgression(""));
		});
		table_job_col_extref.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getExternalReference());
		});
		table_job_col_date_create.setCellValueFactory(p -> {
			long date = p.getValue().getValue().getCreateDate();
			if (date > 0) {
				return new ReadOnlyStringWrapper(date_format.format(new Date(date)));
			} else {
				return new ReadOnlyStringWrapper("");
			}
		});
		table_job_col_date_start.setCellValueFactory(p -> {
			long date = p.getValue().getValue().getStartDate();
			if (date > 0) {
				return new ReadOnlyStringWrapper(date_format.format(new Date(date)));
			} else {
				return new ReadOnlyStringWrapper("");
			}
		});
		table_job_col_date_end.setCellValueFactory(p -> {
			long date = p.getValue().getValue().getEndDate();
			if (date > 0) {
				return new ReadOnlyStringWrapper(date_format.format(new Date(date)));
			} else {
				return new ReadOnlyStringWrapper("");
			}
		});
		
		table_job.setShowRoot(false);
		table_job.setRoot(new TreeItem<>());
		
		table_engine_col_state.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().state);
		});
		table_engine_col_context_type.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().context_type);
		});
		table_engine_col_context_requirement_tags.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().context_requirement_tags);
		});
		table_engine_col_ref.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().ref);
		});
		table_engine_col_progress.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().progress);
		});
		table_engine_col_descr.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().descr);
		});
		
		table_engine.setShowRoot(false);
		table_engine.setRoot(new TreeItem<>());
	}
	
	ObservableList<TreeItem<Job>> getTableJobContent() {
		return table_job.getRoot().getChildren();
	}
	
	void refreshTableJob() {
		table_job.refresh();
	}
	
	ObservableList<TreeItem<TableItemEngineWorker>> getTableEngineContent() {
		return table_engine.getRoot().getChildren();
	}
	
	void refreshTableEngine() {
		table_engine.refresh();
	}
	
	/*
	 **********************
	 * JAVAFX CONTROLS ZONE
	 **********************
	 */
	
	@FXML
	private Button btnclose;
	
	@FXML
	private TreeTableView<Job> table_job;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_name;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_type;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_status;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_progress;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_extref;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_date_create;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_date_start;
	@FXML
	private TreeTableColumn<Job, String> table_job_col_date_end;
	
	@FXML
	private TreeTableView<TableItemEngineWorker> table_engine;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_state;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_context_type;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_context_requirement_tags;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_ref;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_progress;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_descr;
	
	static final class TableItemEngineWorker {
		
		Engine engine;
		WorkerThread worker;
		
		String state = "";
		String context_type = "";
		String context_requirement_tags = "";
		String ref = "";
		String progress = "";
		String descr = "";
		
		void updateEngineOnly() {
			state = String.valueOf(engine.actualFreeWorkers()) + "/" + String.valueOf(engine.maxWorkersCount());
			context_type = engine.getAllHandledContextTypes().stream().collect(Collectors.joining(", "));
			context_requirement_tags = engine.getContextRequirementTags().stream().collect(Collectors.joining(", "));
		}
		
		void updateWorkerOnly() {
			context_type = worker.getJob().getContextType();
			context_requirement_tags = worker.getJob().getContextRequirementTags().stream().collect(Collectors.joining(", "));
			ref = worker.getJob().getKey().toString();
			progress = worker.getJob().getProgression("");
			descr = worker.getJob().getDescription();
		}
		
	}
	
}
