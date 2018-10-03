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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import javafx.application.Platform;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeTableColumn;
import javafx.scene.control.TreeTableView;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;
import tv.hd3g.divergentframework.taskjob.events.EngineEventObserver;
import tv.hd3g.divergentframework.taskjob.events.JobEventObserver;
import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

public class GuiController {
	private static Logger log = LogManager.getLogger();
	
	// private Stage stage;
	// private BorderPane root;
	
	@SuppressWarnings("unused")
	private EventDispatcher event_dispatcher;
	// private Image image_tasks;
	
	private final static SimpleDateFormat date_format = new SimpleDateFormat("HH:mm:ss");
	
	void startApp(Stage stage, BorderPane root, InMemoryLocalTaskJob task_job, Runnable onUserQuit) {
		Scene scene = new Scene(root);
		scene.getStylesheets().add(getClass().getResource("GuiPanel.css").toExternalForm());
		// stage.getIcons().add(new Image(getClass().getResourceAsStream("tasks.png")));
		// image_tasks = new Image(getClass().getResourceAsStream("tasks.png"), 10, 10, false, false);
		
		stage.setScene(scene);
		stage.show();
		
		stage.setOnCloseRequest(event -> {
			event.consume();
			onUserQuit.run();
		});
		
		btnclose.setOnAction(event -> {
			event.consume();
			log.debug("Want to close");
			stage.close();
			onUserQuit.run();
		});
		btnflush.setOnAction(event -> {
			event.consume();
			log.debug("Start manually flush");
			task_job.flush();
		});
		btnchkconsistency.setOnAction(event -> {
			event.consume();
			log.debug("Check manually store consistency");
			task_job.checkStoreConsistency().ifPresentOrElse(e -> {
				/**
				 * On error
				 */
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				
				Alert alert = new Alert(Alert.AlertType.ERROR);
				alert.setTitle("Store consistency selftest");
				alert.setHeaderText("Store consistency is not ok");
				
				TextArea ta = new TextArea(sw.toString());
				ta.setEditable(false);
				ScrollPane sp = new ScrollPane(ta);
				sp.setFitToHeight(true);
				sp.setFitToWidth(true);
				
				alert.getDialogPane().setExpandableContent(sp);
				alert.showAndWait();
			}, () -> {
				/**
				 * On ok
				 */
				Alert alert = new Alert(Alert.AlertType.INFORMATION);
				alert.setTitle("Store consistency selftest");
				alert.setHeaderText("Store consistency is ok");
				alert.showAndWait();
			});
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
			Job job = p.getValue().getValue();
			TaskStatus current_status = job.getStatus();
			if (TaskStatus.ERROR.equals(current_status)) {
				if (job.getLastErrorMessage() != null) {
					if (job.getLastErrorMessage().isEmpty() == false) {
						return new ReadOnlyStringWrapper(current_status.name() + ": " + job.getLastErrorMessage());
					}
				}
			}
			return new ReadOnlyStringWrapper(job.getStatus().name());
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
		table_job_col_date_key.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().getKey().toString());
		});
		
		table_job.setShowRoot(false);
		table_job.setRoot(new TreeItem<>());
		
		GsonBuilder g_b = new GsonBuilder();
		g_b.setPrettyPrinting();
		g_b.disableHtmlEscaping();
		g_b.serializeNulls();
		final Gson gson = g_b.create();
		
		/**
		 * On table_job selection change, update textarea_job_context with the job selected value, or clean it.
		 */
		table_job.getSelectionModel().selectedItemProperty().addListener((observable_value, old_value, new_value) -> {
			if (new_value != null) {
				JsonObject jo_context = new_value.getValue().getContextContent();
				if (jo_context != null) {
					textarea_job_context.setText(gson.toJson(jo_context));
					return;
				}
			}
			textarea_job_context.clear();
		});
		
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
		table_engine_col_job_key.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().job_key);
		});
		table_engine_col_descr.setCellValueFactory(p -> {
			return new ReadOnlyStringWrapper(p.getValue().getValue().descr);
		});
		
		table_engine.setShowRoot(false);
		table_engine.setRoot(new TreeItem<>());
	}
	
	public GuiController linkToTaskJob(InMemoryLocalTaskJob task_job) {
		event_dispatcher = new EventDispatcher(task_job);
		return this;
	}
	
	/*
	 **********************
	 * JAVAFX CONTROLS ZONE
	 **********************
	 */
	
	@FXML
	private Button btnclose;
	@FXML
	private Button btnflush;
	@FXML
	private Button btnchkconsistency;
	
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
	private TreeTableColumn<Job, String> table_job_col_date_key;
	
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
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_job_key;
	@FXML
	private TreeTableColumn<TableItemEngineWorker, String> table_engine_col_descr;
	
	@FXML
	private TextArea textarea_job_context;
	
	private class EventDispatcher implements EngineEventObserver, JobEventObserver {
		private final ExecutorService executor;
		private final ObservableList<TreeItem<Job>> table_job_content = table_job.getRoot().getChildren();
		private final ObservableList<TreeItem<TableItemEngineWorker>> table_engine_content = table_engine.getRoot().getChildren();
		
		private EventDispatcher(InMemoryLocalTaskJob task_job) {
			executor = Executors.newFixedThreadPool(1);
			
			task_job.setEngineObserver(this);
			task_job.setJobObserver(this);
		}
		
		/**
		 * Non blocking for Engine/Event, and polite from JavaFX side.
		 */
		private void exec(Runnable r) {
			executor.execute(() -> {
				Platform.runLater(r);
			});
		}
		
		/**
		 * @param parent is not tested with search
		 * @return never null
		 */
		private /*static*/ Stream<TreeItem<Job>> searchChildrenJob(TreeItem<Job> parent, Predicate<Job> search) {
			Stream<TreeItem<Job>> searched_childrens = parent.getChildren().stream().filter(child -> search.test(child.getValue()));
			
			Stream<TreeItem<Job>> searched_childs_childrens = parent.getChildren().stream().flatMap(child -> {
				return searchChildrenJob(child, search);
			});
			
			return Stream.concat(searched_childrens, searched_childs_childrens);
		}
		
		/**
		 * @return maybe null
		 */
		private TreeItem<Job> getTreeItemByJob(Job job) {
			return getTreeItemByJobUUID(job.getKey());
		}
		
		/**
		 * @return maybe null
		 */
		private TreeItem<Job> getTreeItemByJobUUID(UUID uuid) {
			return Stream.concat(table_job_content.stream().filter(tree_item -> {
				/**
				 * 1st level
				 */
				return tree_item.getValue().getKey().equals(uuid);
			}), table_job_content.stream().flatMap(tree_item -> {
				/**
				 * n levels
				 */
				return searchChildrenJob(tree_item, job_candidate -> {
					return job_candidate.getKey().equals(uuid);
				});
			})).findFirst().orElse(null);
		}
		
		/**
		 * @return maybe null
		 */
		private TreeItem<TableItemEngineWorker> getTreeItemByEngine(Engine engine) {
			return table_engine_content.stream().filter(tree_item -> {
				return tree_item.getValue().engine.equals(engine);
			}).findFirst().orElse(null);
		}
		
		/**
		 * @return maybe null
		 */
		private TreeItem<TableItemEngineWorker> getTreeItemByEngineWorker(Engine engine, WorkerThread worker) {
			TreeItem<TableItemEngineWorker> current_engine_item = getTreeItemByEngine(engine);
			if (current_engine_item == null) {
				return null;
			}
			
			return current_engine_item.getChildren().stream().filter(tree_item -> {
				TableItemEngineWorker e_w = tree_item.getValue();
				return e_w.worker.getJob().equals(worker.getJob());
			}).findFirst().orElse(null);
		}
		
		/**
		 * Same as onJobUpdateProgression
		 */
		public void onJobUpdate(Job job, JobUpdateSubject cause) {
			exec(() -> {
				TreeItem<Job> item = getTreeItemByJob(job);
				if (item == null) {
					log.trace("Can't found job in current job table, maybe it was purged before this update was triggered", () -> job);
					return;
				}
				item.setValue(null);
				item.setValue(job);
			});
		}
		
		/**
		 * Same as onJobUpdate
		 */
		public void onJobUpdateProgression(Job job) {
			exec(() -> {
				TreeItem<Job> item = getTreeItemByJob(job);
				if (item == null) {
					log.trace("Can't found job in current job table, maybe it was purged before this update was triggered", () -> job);
					return;
				}
				item.setValue(null);
				item.setValue(job);
			});
		}
		
		public void brokerOnAfterFlush(List<UUID> deleted_jobs_uuid) {
			exec(() -> {
				deleted_jobs_uuid.forEach(uuid -> {
					TreeItem<Job> item = getTreeItemByJobUUID(uuid);
					if (item == null) {
						log.trace("Don't remove job in table with uuid " + uuid + " because it was deleted");
						return;
					}
					item.getParent().getChildren().remove(item);
				});
			});
		}
		
		public void brokerOnCreateJob(Job job) {
			exec(() -> {
				TreeItem<Job> item = new TreeItem<>(job);
				item.setExpanded(true);
				
				// ImageView iv = new ImageView(image_tasks);
				// iv.setFitHeight(10d);
				// iv.setFitWidth(10d);
				// item.setGraphic(iv);
				table_job_content.add(item);
			});
		}
		
		public void brokerOnCreateSubJob(Job reference, Job sub_job) {
			exec(() -> {
				TreeItem<Job> parent_item = getTreeItemByJob(reference);
				if (parent_item == null) {
					log.error("Create a job (" + sub_job + "), but parent (" + reference + ") was not added in the current tree...");
					return;
				}
				
				TreeItem<Job> item = new TreeItem<>(sub_job);
				item.setExpanded(true);
				parent_item.getChildren().add(item);
			});
		}
		
		public void onRegisterEngine(Engine engine) {
			exec(() -> {
				TableItemEngineWorker _item = new TableItemEngineWorker();
				_item.engine = engine;
				_item.updateEngineOnly();
				
				TreeItem<TableItemEngineWorker> item = new TreeItem<>(_item);
				item.setExpanded(true);
				table_engine_content.add(item);
			});
		}
		
		public void onUnRegisterEngine(Engine engine) {
			exec(() -> {
				TreeItem<TableItemEngineWorker> item = getTreeItemByEngine(engine);
				item.getParent().getChildren().remove(item);
			});
		}
		
		/**
		 * Same as onEngineStop
		 */
		public void onEngineChangeContextRequirementTags(Engine engine) {
			exec(() -> {
				TreeItem<TableItemEngineWorker> item = getTreeItemByEngine(engine);
				if (item == null) {
					log.error("Can't found engine " + engine);
					return;
				}
				TableItemEngineWorker v = item.getValue();
				v.updateEngineOnly();
				item.setValue(null);
				item.setValue(v);
			});
		}
		
		/**
		 * Same as onEngineChangeContextRequirementTags
		 */
		public void onEngineStop(Engine engine) {
			exec(() -> {
				TreeItem<TableItemEngineWorker> item = getTreeItemByEngine(engine);
				if (item == null) {
					log.error("Can't found engine " + engine);
					return;
				}
				TableItemEngineWorker v = item.getValue();
				v.updateEngineOnly();
				item.setValue(null);
				item.setValue(v);
			});
		}
		
		public void onEngineStartProcess(Engine engine, WorkerThread w_t) {
			exec(() -> {
				TreeItem<TableItemEngineWorker> item = getTreeItemByEngine(engine);
				if (item == null) {
					log.error("Can't found engine " + engine);
					return;
				}
				
				TableItemEngineWorker _worker = new TableItemEngineWorker();
				_worker.engine = engine;
				_worker.worker = w_t;
				_worker.updateWorkerOnly();
				
				TreeItem<TableItemEngineWorker> item_worker = new TreeItem<>(_worker);
				item.getChildren().add(item_worker);
			});
		}
		
		public void onEngineEndsProcess(Engine engine, WorkerThread w_t) {
			exec(() -> {
				TreeItem<TableItemEngineWorker> item_worker = getTreeItemByEngineWorker(engine, w_t);
				if (item_worker == null) {
					log.error("Can't found worker for " + engine);
					return;
				}
				
				item_worker.getParent().getChildren().remove(item_worker);
			});
		}
		
	}
	
}
