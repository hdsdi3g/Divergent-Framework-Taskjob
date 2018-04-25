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
package tv.hd3g.taskjob.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

import junit.framework.TestCase;
import tv.hd3g.taskjob.broker.Broker;
import tv.hd3g.taskjob.broker.Job;
import tv.hd3g.taskjob.broker.JobUtilityTest;
import tv.hd3g.taskjob.broker.TaskStatus;
import tv.hd3g.taskjob.worker.Engine;

public class TestLiveQueue extends TestCase {
	
	private class TestBroker implements Broker {
		
		private final List<Job> all_jobs;
		// private volatile Runnable local_jobs_activity_callback;
		
		public TestBroker(List<Job> all_jobs) {
			this.all_jobs = all_jobs;
			if (all_jobs == null) {
				throw new NullPointerException("\"all_jobs\" can't to be null");
			}
			
		}
		
		public void updateProgression(Job action, int actual_value, int max_value) {
			throw new RuntimeException("Not implemented");
		}
		
		public void switchToError(Job job, Throwable e) {
			JobUtilityTest.switchToError(job, e);
		}
		
		public void switchStatus(Job job, TaskStatus new_status) {
			JobUtilityTest.switchStatus(job, new_status);
		}
		
		public void registerCallbackOnNewLocalJobsActivity(Runnable callback) {
			// local_jobs_activity_callback = callback;
		}
		
		public List<Job> getJobsByUUID(List<UUID> keys) {
			throw new RuntimeException("Not implemented");
		}
		
		public List<Job> getAllJobs() {
			return all_jobs;
		}
		
		public Job createJob(String description, String external_reference, String context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
			throw new RuntimeException("Not implemented");
		}
		
		public Job addSubJob(Job reference, String description, String external_reference, String context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
			throw new RuntimeException("Not implemented");
		}
		
		public void getNextJobs(List<String> list_to_context_types, IntSupplier queue_capacity, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundActionReadyToStart) {
			all_jobs.stream().takeWhile(job -> {
				return queue_capacity.getAsInt() > 0;
			}).filter(job -> {
				return job.getStatus().equals(TaskStatus.WAITING);
			}).filter(job -> {
				return list_to_context_types.contains(job.getContextType());
			}).filter(job -> {
				return filterByContextTypeAndTags.test(job.getContextType(), job.getContextRequirementTags());
			}).forEach(job -> {
				onFoundActionReadyToStart.test(job);
			});
		}
		
	};
	
	private Job createJob(String context_type, String... context_requirement_tags) {
		JsonObject jo = new JsonObject();
		jo.addProperty("expected", context_type);
		
		ArrayList<String> al_context_requirement_tags = new ArrayList<>();
		for (int pos = 0; pos < context_requirement_tags.length; pos++) {
			al_context_requirement_tags.add(context_requirement_tags[pos]);
		}
		
		return JobUtilityTest.createJob("test", context_type, jo, al_context_requirement_tags);
	}
	
	public void test() throws Exception {
		
		ArrayList<Job> all_jobs = new ArrayList<>();
		TestBroker broker = new TestBroker(all_jobs);
		
		all_jobs.addAll(IntStream.range(0, 10).mapToObj(i -> {
			if (i % 2 == 0) {
				return createJob("context1");
			} else {
				return createJob("context2");
			}
		}).collect(Collectors.toList()));
		
		LiveQueue queue = new LiveQueue(broker);
		
		Engine engine_1 = new Engine(1, "E1", Arrays.asList("context1"), c_type -> {
			return (referer, bkr, shouldStopProcessing) -> {
				JobUtilityTest.addPropertyInContext(referer, "done", "context1");
				assertEquals("context1", referer.getContextContent().get("expected").getAsString());
				Thread.sleep(1);
			};
		});
		
		Engine engine_2 = new Engine(1, "E2", Arrays.asList("context2"), c_type -> {
			return (referer, bkr, shouldStopProcessing) -> {
				JobUtilityTest.addPropertyInContext(referer, "done", "context2");
				assertEquals("context2", referer.getContextContent().get("expected").getAsString());
				Thread.sleep(1);
			};
		});
		
		/**
		 * Only for cosmetics use
		 */
		Logger.getLogger("tv.hd3g.taskjob.worker.WorkerThread").setLevel(Level.WARN);
		
		queue.registerEngine(engine_1);
		queue.registerEngine(engine_2);
		
		assertTrue(queue.isRunning());
		assertTrue(queue.getActualEnginesContextTypes(false).contains("context1"));
		assertTrue(queue.getActualEnginesContextTypes(false).contains("context2"));
		assertTrue(queue.getActualEnginesContextTypes(true).isEmpty());
		
		while (queue.isRunning()) {
			Thread.onSpinWait();
		}
		
		Thread.sleep(100);
		
		if (all_jobs.stream().anyMatch(job -> {
			return job.getStatus() != TaskStatus.DONE;
		})) {
			all_jobs.stream().filter(job -> {
				return job.getStatus() != TaskStatus.DONE;
			}).forEach(job -> {
				System.out.println(job.getStatus() + "\t" + job.getContextType());
			});
			
			fail("Some jobs are not done");
		}
		
		if (all_jobs.stream().allMatch(job -> {
			return job.getContextType().equals(job.getContextContent().get("done").getAsString());
		}) == false) {
			fail("Mergue context types");
		}
		
		queue.unRegisterEngine(engine_1);
		queue.registerEngine(engine_1);
		queue.unRegisterEngine(engine_1);
		
		assertEquals(1, queue.getActualEnginesContextTypes(false).size());
		assertEquals("context2", queue.getActualEnginesContextTypes(false).get(0));
		
		queue.unRegisterEngine(engine_2);
		
		assertTrue(queue.getActualEnginesContextTypes(false).isEmpty());
		
		assertFalse(queue.isPendingStop());
		queue.prepareToStop(ForkJoinPool.commonPool()).get(1, TimeUnit.SECONDS);
		assertTrue(queue.isPendingStop());
		
		RuntimeException expected_error = null;
		try {
			queue.registerEngine(engine_1);
		} catch (RuntimeException e) {
			expected_error = e;
		}
		assertNotNull(expected_error);
		
	}
	
	public void testIgnoredContext() throws Exception {
		
		ArrayList<Job> all_jobs = new ArrayList<>();
		all_jobs.add(createJob("other_context"));
		TestBroker broker = new TestBroker(all_jobs);
		
		LiveQueue queue = new LiveQueue(broker);
		
		Engine engine_1 = new Engine(1, "E1", Arrays.asList("context1"), c_type -> {
			return (referer, bkr, shouldStopProcessing) -> {
				JobUtilityTest.addPropertyInContext(referer, "done", "context1");
				assertEquals("context1", referer.getContextContent().get("expected").getAsString());
			};
		});
		
		queue.registerEngine(engine_1);
		
		assertFalse(queue.isRunning());
		assertTrue(queue.getActualEnginesContextTypes(false).contains("context1"));
		assertFalse(queue.getActualEnginesContextTypes(false).contains("other_context"));
		assertFalse(queue.getActualEnginesContextTypes(true).isEmpty());
		
		while (queue.isRunning()) {
			Thread.onSpinWait();
		}
		Thread.sleep(100);
		
		assertEquals(TaskStatus.WAITING, all_jobs.get(0).getStatus());
	}
	
	public void testRequiredContext() throws Exception {
		Job job_1_no_rct = createJob("context1");
		Job job_2_simple_rct = createJob("context1", "r0");
		Job job_3_missing_rct = createJob("context1", "rNOPE");
		Job job_4_present_and_missing_rct = createJob("context1", "r0", "rNOPE");
		Job job_5_duo_rct = createJob("context1", "r0", "r1");
		
		ArrayList<Job> all_jobs = new ArrayList<>();
		all_jobs.add(job_1_no_rct);
		all_jobs.add(job_2_simple_rct);
		all_jobs.add(job_3_missing_rct);
		all_jobs.add(job_4_present_and_missing_rct);
		all_jobs.add(job_5_duo_rct);
		TestBroker broker = new TestBroker(all_jobs);
		
		LiveQueue queue = new LiveQueue(broker);
		
		Engine engine_1 = new Engine(1, "E1", Arrays.asList("context1"), c_type -> {
			return (referer, bkr, shouldStopProcessing) -> {
				JobUtilityTest.addPropertyInContext(referer, "done", "context1");
				assertEquals("context1", referer.getContextContent().get("expected").getAsString());
			};
		});
		engine_1.setContextRequirementTags(Arrays.asList("r0", "r1"));
		
		queue.registerEngine(engine_1);
		
		assertTrue(queue.isRunning());
		while (queue.isRunning()) {
			Thread.onSpinWait();
		}
		
		Thread.sleep(100);
		
		assertEquals(TaskStatus.DONE, job_1_no_rct.getStatus());
		assertEquals(TaskStatus.DONE, job_2_simple_rct.getStatus());
		assertEquals(TaskStatus.WAITING, job_3_missing_rct.getStatus());
		assertEquals(TaskStatus.WAITING, job_4_present_and_missing_rct.getStatus());
		assertEquals(TaskStatus.DONE, job_5_duo_rct.getStatus());
	}
	
}
