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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.scene.control.TreeItem;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.events.EngineEventObserver;
import tv.hd3g.divergentframework.taskjob.events.JobEventObserver;
import tv.hd3g.divergentframework.taskjob.gui.GuiController.TableItemEngineWorker;
import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

public class GuiEventDispatcher implements EngineEventObserver, JobEventObserver {
	
	private static Logger log = LogManager.getLogger();
	
	private final GuiController controller;
	private final ExecutorService executor;
	
	private final ObservableList<TreeItem<Job>> table_job_content;
	private final ObservableList<TreeItem<TableItemEngineWorker>> table_engine_content;
	
	GuiEventDispatcher(GuiController controller, InMemoryLocalTaskJob task_job) {
		this.controller = controller;
		executor = Executors.newFixedThreadPool(1);
		task_job.setEngineObserver(this);
		task_job.setJobObserver(this);
		
		table_job_content = controller.getTableJobContent();
		table_engine_content = controller.getTableEngineContent();
	}
	
	/**
	 * Non blocking for Engine/Event, and polite from JajaFX side.
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
	private static Stream<TreeItem<Job>> searchChildrenJob(TreeItem<Job> parent, Predicate<Job> search) {
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
				log.error("Can't found job in current job table" + job);
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
				log.error("Can't found job in current job table" + job);
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
	
	// TODO update workers states: set value null && set value previous
	
}
