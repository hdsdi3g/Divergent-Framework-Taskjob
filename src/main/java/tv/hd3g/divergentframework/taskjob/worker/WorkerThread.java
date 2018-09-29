/*
 * This file is part of Divergent-Framework-Taskjob.
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
package tv.hd3g.divergentframework.taskjob.worker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import tv.hd3g.divergentframework.taskjob.broker.Broker;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;

/**
 * Execute a Worker in a Thread
 */
public class WorkerThread extends Thread {
	private static final Logger log = LogManager.getLogger();
	
	private final Job job;
	private final Broker broker;
	private final Worker worker;
	private volatile boolean want_to_stop;
	
	private Runnable afterProcess;
	
	WorkerThread(String name, Job job, Broker broker, Worker worker) {
		setDaemon(true);
		setPriority(Thread.MIN_PRIORITY);
		setName(name);
		
		this.job = job;
		if (job == null) {
			throw new NullPointerException("\"job\" can't to be null");
		}
		this.broker = broker;
		if (broker == null) {
			throw new NullPointerException("\"broker\" can't to be null");
		}
		this.worker = worker;
		if (worker == null) {
			throw new NullPointerException("\"worker\" can't to be null");
		}
		want_to_stop = false;
	}
	
	/**
	 * Will always started even if it has some errors or if it's stopped.
	 * @return this
	 */
	public WorkerThread setAfterProcess(Runnable afterProcess) {
		this.afterProcess = afterProcess;
		return this;
	}
	
	public void run() {
		log.info("Start worker process, \"" + job.getContextType() + "\" by " + worker + " for " + job.getKey().toString().substring(0, 8));
		
		Runnable onStopProcessing = worker.onStopProcessing();
		
		broker.switchStatus(job, TaskStatus.PROCESSING);
		
		try {
			worker.process(job, broker, () -> want_to_stop);
			
			if (want_to_stop) {
				if (onStopProcessing != null) {
					broker.switchStatus(job, TaskStatus.STOPPING);
					
					try {
						onStopProcessing.run();
					} catch (Exception e) {
						log.warn("Can't execute onStopProcessing in worker " + worker + " for " + job, e);
					}
				}
				broker.switchStatus(job, TaskStatus.STOPPED);
			} else {
				broker.switchStatus(job, TaskStatus.DONE);
			}
		} catch (Throwable e) {
			log.error("Process error", e);
			broker.switchToError(job, e);
		}
		
		if (afterProcess != null) {
			afterProcess.run();
		}
	}
	
	/**
	 * Non blocking
	 */
	void wantToStop() {
		want_to_stop = true;
	}
	
	public CompletableFuture<Void> waitToStop(Executor executor) {
		want_to_stop = true;
		
		return CompletableFuture.runAsync(() -> {
			log.debug("Wait worker " + worker + " ends...");
			while (isAlive()) {
				Thread.onSpinWait();
			}
		}, executor);
	}
	
	public String toString() {
		if (want_to_stop && isAlive()) {
			return "Alive worker want to stop... " + getName() + ", " + worker.getClass().getSimpleName() + " for " + job.getContextType() + " [" + job.getKey() + "]";
		} else if (isAlive()) {
			return "Alive worker " + getName() + ", " + worker.getClass().getSimpleName() + " for " + job.getContextType() + " [" + job.getKey() + "]";
		} else {
			return "Worker (idle) " + worker.getClass().getSimpleName() + " for " + job.getContextType() + " [" + job.getKey() + "]";
		}
	}
	
	public Job getJob() {
		return job;
	}
}
