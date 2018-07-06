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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.gson.JsonObject;

import junit.framework.TestCase;
import tv.hd3g.divergentframework.taskjob.broker.Broker;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.JobUtilityTest;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;

public class TestWorkerThread extends TestCase {
	
	private JsonObject createContext(long msec_timer) {
		JsonObject jo = new JsonObject();
		jo.addProperty("msec", msec_timer);
		return jo;
	}
	
	private class TestWorker implements Worker {
		
		private AtomicInteger stopped = new AtomicInteger(0);
		private boolean last_should_stop_processing = false;
		
		public void process(Job referer, Broker brkr, Supplier<Boolean> shouldStopProcessing) throws Throwable {
			assertEquals(JobUtilityTest.broker, brkr);
			assertTrue(referer.getContextContent().has("msec"));
			
			long msec = referer.getContextContent().get("msec").getAsLong();
			
			if (referer.getContextContent().has("error")) {
				throw new RuntimeException("Error test");
			}
			
			while (msec > 0 && shouldStopProcessing.get() == false) {
				Thread.sleep(1);
				msec--;
			}
			
			last_should_stop_processing = shouldStopProcessing.get();
		}
		
		public Runnable onStopProcessing() {
			return () -> {
				stopped.incrementAndGet();
			};
		}
		
	}
	
	public void testExec() {
		Job job = JobUtilityTest.createJob("test", "test", createContext(10), null);
		JobUtilityTest.switchStatus(job, TaskStatus.PREPARING);
		
		TestWorker worker = new TestWorker();
		WorkerThread w_t = new WorkerThread("Test", job, JobUtilityTest.broker, worker);
		w_t.start();
		
		while (w_t.isAlive()) {
			Thread.onSpinWait();
		}
		
		assertEquals(0, worker.stopped.get());
		assertFalse(worker.last_should_stop_processing);
		assertEquals(TaskStatus.DONE, job.getStatus());
	}
	
	public void testStopSimple() {
		Job job = JobUtilityTest.createJob("test", "test", createContext(10), null);
		JobUtilityTest.switchStatus(job, TaskStatus.PREPARING);
		
		TestWorker worker = new TestWorker();
		WorkerThread w_t = new WorkerThread("Test", job, JobUtilityTest.broker, worker);
		w_t.start();
		
		w_t.wantToStop();
		
		while (w_t.isAlive()) {
			Thread.onSpinWait();
		}
		
		assertEquals(1, worker.stopped.get());
		assertTrue(worker.last_should_stop_processing);
		assertEquals(TaskStatus.STOPPED, job.getStatus());
	}
	
	public void testStopSync() throws InterruptedException, ExecutionException {
		Job job = JobUtilityTest.createJob("test", "test", createContext(10), null);
		JobUtilityTest.switchStatus(job, TaskStatus.PREPARING);
		
		TestWorker worker = new TestWorker();
		WorkerThread w_t = new WorkerThread("Test", job, JobUtilityTest.broker, worker);
		w_t.start();
		
		w_t.waitToStop(ForkJoinPool.commonPool()).get();
		
		assertFalse(w_t.isAlive());
		assertEquals(1, worker.stopped.get());
		assertTrue(worker.last_should_stop_processing);
		assertEquals(TaskStatus.STOPPED, job.getStatus());
	}
	
	public void testAfterProcess() {
		Job job = JobUtilityTest.createJob("test", "test", createContext(10), null);
		JobUtilityTest.switchStatus(job, TaskStatus.PREPARING);
		
		TestWorker worker = new TestWorker();
		WorkerThread w_t = new WorkerThread("Test", job, JobUtilityTest.broker, worker);
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		w_t.setAfterProcess(() -> {
			assertEquals(w_t, Thread.currentThread());
			trigger_after_process.getAndIncrement();
		});
		w_t.start();
		
		while (w_t.isAlive()) {
			Thread.onSpinWait();
		}
		
		assertEquals(0, worker.stopped.get());
		assertFalse(worker.last_should_stop_processing);
		assertEquals(TaskStatus.DONE, job.getStatus());
		assertEquals(1, trigger_after_process.get());
	}
	
	public void testRuntimeError() {
		JsonObject jo_context = createContext(10);
		jo_context.addProperty("error", 1);
		
		Job job = JobUtilityTest.createJob("test", "test", jo_context, null);
		JobUtilityTest.switchStatus(job, TaskStatus.PREPARING);
		
		TestWorker worker = new TestWorker();
		WorkerThread w_t = new WorkerThread("Test", job, JobUtilityTest.broker, worker);
		
		AtomicInteger trigger_after_process = new AtomicInteger(0);
		w_t.setAfterProcess(() -> {
			assertEquals(w_t, Thread.currentThread());
			trigger_after_process.getAndIncrement();
		});
		
		w_t.start();
		
		while (w_t.isAlive()) {
			Thread.onSpinWait();
		}
		
		assertEquals(0, worker.stopped.get());
		assertFalse(worker.last_should_stop_processing);
		assertEquals(TaskStatus.ERROR, job.getStatus());
		assertEquals(1, trigger_after_process.get());
	}
	
}
