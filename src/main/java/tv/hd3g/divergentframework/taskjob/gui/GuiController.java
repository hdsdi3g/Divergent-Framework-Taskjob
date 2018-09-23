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

public class GuiController {
	private static Logger log = LogManager.getLogger();
	
	private Stage stage;
	private BorderPane root;
	
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
			log.info("log me");
			event.consume();
		});
		
		tree_table_col_name.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getDescription());
		});
		
		tree_table_col_type.setCellValueFactory(p -> {
			String ctx_type = p.getValue().getValue().getContextType();
			if (ctx_type.startsWith(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE)) {
				return new ReadOnlyStringWrapper(ctx_type.substring(Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE.length()));
			} else {
				return new ReadOnlyStringWrapper(ctx_type + " (not java)");
			}
		});
		
		tree_table_col_status.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getStatus().name());
		});
		
		tree_table_col_progress.setCellValueFactory(p -> {
			int progression = p.getValue().getValue().getActualProgressionValue();
			int max = p.getValue().getValue().getMaxProgressionValue();
			
			if (max > 0) {
				return new ReadOnlyStringWrapper(String.valueOf(progression) + "/" + String.valueOf(max));
			} else {
				if (progression > 0) {
					return new ReadOnlyStringWrapper(String.valueOf(progression));
				} else {
					return new ReadOnlyStringWrapper("");
				}
			}
		});
		
		tree_table_col_extref.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getExternalReference());
		});
		
		final SimpleDateFormat date_format = new SimpleDateFormat("HH:mm:ss");
		
		tree_table_col_date_create.setCellValueFactory(p -> {
			long date = p.getValue().getValue().getCreateDate();
			if (date > 0) {
				return new ReadOnlyStringWrapper(date_format.format(new Date(date)));
			} else {
				return new ReadOnlyStringWrapper("");
			}
		});
		tree_table_col_date_start.setCellValueFactory(p -> {
			long date = p.getValue().getValue().getStartDate();
			if (date > 0) {
				return new ReadOnlyStringWrapper(date_format.format(new Date(date)));
			} else {
				return new ReadOnlyStringWrapper("");
			}
		});
		tree_table_col_date_end.setCellValueFactory(p -> {
			long date = p.getValue().getValue().getEndDate();
			if (date > 0) {
				return new ReadOnlyStringWrapper(date_format.format(new Date(date)));
			} else {
				return new ReadOnlyStringWrapper("");
			}
		});
		
		tree_table_jobs.setShowRoot(false);
		tree_table_jobs.setRoot(new TreeItem<>());
	}
	
	public ObservableList<TreeItem<Job>> getTableJobContent() {
		return tree_table_jobs.getRoot().getChildren();
	}
	
	/*
	 **********************
	 * JAVAFX CONTROLS ZONE
	 **********************
	 */
	
	@FXML
	private Button btnclose;
	
	@FXML
	private TreeTableView<Job> tree_table_jobs;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_name;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_type;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_status;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_progress;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_extref;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_date_create;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_date_start;
	@FXML
	private TreeTableColumn<Job, String> tree_table_col_date_end;
	
}
