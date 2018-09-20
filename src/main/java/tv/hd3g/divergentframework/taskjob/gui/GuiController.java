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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javafx.fxml.FXML;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

public class GuiController {
	private static Logger log = LogManager.getLogger();
	
	private Stage stage;
	private BorderPane root;
	
	@FXML
	private Button btnclose;
	// private javafx.collections.ObservableList<FileToSend> list_rows_files_to_send;//FXCollections.observableArrayList();
	
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
	}
	
}
