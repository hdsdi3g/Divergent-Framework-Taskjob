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
import java.util.function.BiPredicate;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

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
		private volatile Runnable local_jobs_activity_callback;
		
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
		
		public void onNewLocalJobsActivity(Runnable callback) {
			local_jobs_activity_callback = callback;
		}
		
		public List<Job> getJobsByUUID(Job job, List<UUID> keys) {
			throw new RuntimeException("Not implemented");
		}
		
		public List<Job> getAllJobs() {
			return all_jobs;
		}
		
		public Job createJob(String description, String external_reference, JsonObject context, ArrayList<String> context_requirement_tags) {
			throw new RuntimeException("Not implemented");
		}
		
		public Job addSubJob(Job reference, String description, JsonObject context, ArrayList<String> context_requirement_tags) {
			throw new RuntimeException("Not implemented");
		}
		
		public void getNextActions(List<String> list_to_context_types, IntSupplier queue_capacity, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundActionReadyToStart) {
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
	
	public void test() {
		
		ArrayList<Job> all_jobs = new ArrayList<>();
		TestBroker broker = new TestBroker(all_jobs);
		
		// XXX all_jobs.add(e)
		
		LiveQueue queue = new LiveQueue(broker);
		
		Engine engine_1 = new Engine(1, "E1", Arrays.asList("context1"), c_type -> {
			return (referer, bkr, shouldStopProcessing) -> {
				referer.getContextContent().addProperty("done", "context1");
			};
		});
		
		Engine engine_2 = new Engine(1, "E2", Arrays.asList("context2"), c_type -> {
			return (referer, bkr, shouldStopProcessing) -> {
				referer.getContextContent().addProperty("done", "context2");
			};
		});
		
		queue.registerEngine(engine_1);
		queue.registerEngine(engine_2);
		
		assertTrue(queue.getActualEnginesContextTypes().contains("context1"));
		assertTrue(queue.getActualEnginesContextTypes().contains("context2"));
		
		// XXX assert jobs created with contextX are == job.getContextContent().get("contextX").getAsString();
		
		// XXX queue.unRegisterEngine(engine);
		// XXX queue.prepareToStop(ForkJoinPool.commonPool());
		// XXX queue.isStopped();
		// XXX queue.isPendingStop();
	}
	
	// XXX test with engine r_tags
	// XXX test with context collision between engines
}
