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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.JobUtilityTest;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;

public class TestEngine extends TestCase {
	
	private JsonObject createContext(boolean sleep) {
		JsonObject jo = new JsonObject();
		if (sleep) {
			jo.addProperty("sleep", true);
		}
		return jo;
	}
	
	public void testExec() throws Exception {
		CountDownLatch latch_end = new CountDownLatch(1);
		AtomicReference<AssertionFailedError> failure = new AtomicReference<>();
		
		Engine engine = new Engine(1, "Test", Arrays.asList("test"), c_type -> {
			return (referer, broker, shouldStopProcessing) -> {
				try {
					assertEquals("test", referer.getContextType());
					assertEquals(TaskStatus.PROCESSING, referer.getStatus());
					assertFalse("Job was previously done", referer.getContextContent().has("done"));
					JobUtilityTest.addPropertyInContext(referer, "done", true);
					
					if (referer.getContextContent().has("sleep")) {
						Thread.sleep(10);
					}
				} catch (AssertionFailedError e) {
					failure.set(e);
					latch_end.countDown();
				}
			};
		});
		
		assertEquals(1, engine.actualFreeWorkers());
		assertEquals(1, engine.getAllHandledContextTypes().size());
		assertEquals("test", engine.getAllHandledContextTypes().get(0));
		
		assertNotNull(engine.getContextRequirementTags());
		engine.setContextRequirementTags(Arrays.asList("t1"));
		assertEquals(1, engine.getContextRequirementTags().size());
		assertEquals("t1", engine.getContextRequirementTags().get(0));
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		
		Job job = JobUtilityTest.createJob("Test", "test", createContext(true), null);
		
		assertTrue(engine.addProcess(job, JobUtilityTest.broker, () -> {
			trigger_after_process.getAndIncrement();
		}));
		
		assertTrue(engine.isRunning());
		assertEquals(0, engine.actualFreeWorkers());
		
		CompletableFuture.runAsync(() -> {
			while (engine.isRunning()) {
				Thread.onSpinWait();
			}
			latch_end.countDown();
		});
		
		latch_end.await(1, TimeUnit.SECONDS);
		if (failure.get() != null) {
			throw failure.get();
		}
		
		assertEquals(1, engine.actualFreeWorkers());
		assertEquals(TaskStatus.DONE, job.getStatus());
		assertTrue(job.getContextContent().has("done"));
		assertTrue(job.getContextContent().get("done").getAsBoolean());
		
		/**
		 * Let the last thread to exec
		 */
		Thread.sleep(1);
		assertEquals(1, trigger_after_process.get());
	}
	
	private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
	
	public void testMultiple() throws Exception {
		CountDownLatch latch_end = new CountDownLatch(1);
		AtomicReference<AssertionFailedError> failure = new AtomicReference<>();
		
		Engine engine = new Engine(CPU_COUNT, "Test", Arrays.asList("test"), c_type -> {
			return (referer, broker, shouldStopProcessing) -> {
				try {
					assertEquals("test", referer.getContextType());
					assertEquals(TaskStatus.PROCESSING, referer.getStatus());
					assertFalse("Job was previously done", referer.getContextContent().has("done"));
					JobUtilityTest.addPropertyInContext(referer, "done", true);
				} catch (AssertionFailedError e) {
					failure.set(e);
					latch_end.countDown();
				}
			};
		});
		
		assertEquals(CPU_COUNT, engine.actualFreeWorkers());
		
		Logger.getLogger(WorkerThread.class).setLevel(Level.WARN);
		
		List<Job> all_jobs = IntStream.range(0, CPU_COUNT * 1000).mapToObj(i -> {
			return JobUtilityTest.createJob("Test-" + i, "test", createContext(false), null);
		}).collect(Collectors.toList());
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		
		Runnable triggerAfterProcess = () -> {
			trigger_after_process.getAndIncrement();
		};
		
		System.out.println("Start to exec " + all_jobs.size() + " jobs");
		all_jobs.parallelStream().forEach(job -> {
			while (engine.addProcess(job, JobUtilityTest.broker, triggerAfterProcess) == false) {
				Thread.onSpinWait();
				
				if (failure.get() != null) {
					throw failure.get();
				}
			}
		});
		
		CompletableFuture.runAsync(() -> {
			while (engine.isRunning()) {
				Thread.onSpinWait();
			}
			latch_end.countDown();
		});
		
		latch_end.await(10, TimeUnit.SECONDS);
		if (failure.get() != null) {
			throw failure.get();
		}
		
		assertEquals(CPU_COUNT, engine.actualFreeWorkers());
		assertEquals(all_jobs.size(), trigger_after_process.get());
		
		assertTrue(all_jobs.parallelStream().anyMatch(job -> {
			return TaskStatus.DONE.equals(job.getStatus()) & job.getContextContent().has("done");
		}));
	}
	
	public void testStop() throws Exception {
		
		AtomicBoolean stopped = new AtomicBoolean(false);
		
		Engine engine = new Engine(1, "Test", Arrays.asList("test"), c_type -> {
			return (referer, broker, shouldStopProcessing) -> {
				Thread.sleep(10);
				JobUtilityTest.addPropertyInContext(referer, "done", true);
				
				if (shouldStopProcessing.get() == false) {
					Thread.sleep(10);
					stopped.set(true);
				}
			};
		});
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		
		Job job = JobUtilityTest.createJob("Test", "test", createContext(false), null);
		
		engine.addProcess(job, JobUtilityTest.broker, () -> {
			trigger_after_process.getAndIncrement();
		});
		
		assertTrue(engine.isRunning());
		assertEquals(0, engine.actualFreeWorkers());
		
		/**
		 * Do stop now
		 */
		engine.stopCurrentAll();
		
		while (engine.isRunning()) {
			Thread.onSpinWait();
		}
		
		assertEquals(1, engine.actualFreeWorkers());
		assertEquals(TaskStatus.STOPPED, job.getStatus());
		assertTrue(job.getContextContent().has("done"));
		assertTrue(job.getContextContent().get("done").getAsBoolean());
		
		/**
		 * Let the last thread to exec
		 */
		Thread.sleep(1);
		assertEquals(1, trigger_after_process.get());
	}
	
	public void testStopSync() throws Exception {
		
		AtomicBoolean stopped = new AtomicBoolean(false);
		
		Engine engine = new Engine(1, "Test", Arrays.asList("test"), c_type -> {
			return (referer, broker, shouldStopProcessing) -> {
				Thread.sleep(10);
				JobUtilityTest.addPropertyInContext(referer, "done", true);
				
				if (shouldStopProcessing.get() == false) {
					Thread.sleep(10);
					stopped.set(true);
				}
			};
		});
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		
		Job job = JobUtilityTest.createJob("Test", "test", createContext(false), null);
		
		engine.addProcess(job, JobUtilityTest.broker, () -> {
			trigger_after_process.getAndIncrement();
		});
		
		assertTrue(engine.isRunning());
		assertEquals(0, engine.actualFreeWorkers());
		
		/**
		 * Stop now
		 */
		engine.stopCurrentAll(ForkJoinPool.commonPool()).get();
		
		assertEquals(1, engine.actualFreeWorkers());
		assertEquals(TaskStatus.STOPPED, job.getStatus());
		assertTrue(job.getContextContent().has("done"));
		assertTrue(job.getContextContent().get("done").getAsBoolean());
		
		/**
		 * Let the last thread to exec
		 */
		Thread.sleep(1);
		assertEquals(1, trigger_after_process.get());
	}
	
	public void testInvalidContext() throws Exception {
		
		Engine engine = new Engine(1, "Test", Arrays.asList("test"), c_type -> {
			return (referer, broker, shouldStopProcessing) -> {
				JobUtilityTest.addPropertyInContext(referer, "done", true);
			};
		});
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		
		Job job = JobUtilityTest.createJob("Test", "other-context", createContext(false), null);
		
		RuntimeException expected_error = null;
		try {
			engine.addProcess(job, JobUtilityTest.broker, () -> {
				trigger_after_process.getAndIncrement();
			});
		} catch (RuntimeException e) {
			expected_error = e;
		}
		
		assertNotNull(expected_error);
		assertEquals(1, engine.actualFreeWorkers());
		assertEquals(TaskStatus.WAITING, job.getStatus());
		assertFalse(job.getContextContent().has("done"));
		assertEquals(0, trigger_after_process.get());
	}
	
}
