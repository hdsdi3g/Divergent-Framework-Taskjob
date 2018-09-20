/*
 * This file is part of Divergent Framework Taskjob.
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
package tv.hd3g.divergentframework.taskjob.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.gson.JsonObject;

import junit.framework.TestCase;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.InMemoryJobStore;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;

public class TestJobStore extends TestCase {
	
	public void testPushPull() {
		InMemoryJobStore store = new InMemoryJobStore();
		
		Job job = JobUtilityTest.createJob("Test", "Test", new JsonObject(), null);
		assertTrue(store.put(job));
		
		Job job_get = store.getByUUID(job.getKey());
		assertNotNull(job_get);
		
		assertEquals(job, job_get);
		
		assertEquals(1, store.getJobsByTaskStatus(TaskStatus.WAITING).size());
		assertEquals(job, store.getJobsByTaskStatus(TaskStatus.WAITING).get(0));
		assertEquals(0, store.getJobsByTaskStatus(TaskStatus.POSTPONED).size());
		
		JobUtilityTest.switchStatus(job, TaskStatus.POSTPONED);
		
		assertEquals(0, store.getJobsByTaskStatus(TaskStatus.WAITING).size());
		assertEquals(0, store.getJobsByTaskStatus(TaskStatus.POSTPONED).size());
		
		assertTrue(store.update(() -> {
			return job.getKey();
		}));
		
		assertEquals(0, store.getJobsByTaskStatus(TaskStatus.WAITING).size());
		assertEquals(1, store.getJobsByTaskStatus(TaskStatus.POSTPONED).size());
		assertEquals(job, store.getJobsByTaskStatus(TaskStatus.POSTPONED).get(0));
		
		Optional<Job> o_job = store.getFromJobs(TaskStatus.POSTPONED, stream -> {
			return stream.findFirst();
		});
		assertEquals(job, o_job.get());
		
		store.checkConsistency();
	}
	
	public void testParallelPushPull() {
		List<Job> all_jobs = IntStream.range(0, 100_000).mapToObj(i -> JobUtilityTest.createJob("Test", "Test", new JsonObject(), null)).collect(Collectors.toList());
		
		InMemoryJobStore store = new InMemoryJobStore();
		
		/**
		 * Test bulk simple put
		 */
		all_jobs.parallelStream().forEach(job -> {
			assertTrue(store.put(job));
		});
		
		assertEquals(all_jobs.size(), store.size());
		
		/**
		 * Test bulk simple get
		 */
		all_jobs.parallelStream().forEach(job -> {
			Job job_get = store.getByUUID(job.getKey());
			assertNotNull(job_get);
			assertEquals(job, job_get);
		});
		
		List<Job> all_waiting = store.getJobsByTaskStatus(TaskStatus.WAITING);
		assertEquals(all_jobs.size(), all_waiting.size());
		
		Set<UUID> all_jobs_uuid = all_jobs.parallelStream().map(job -> job.getKey()).collect(Collectors.toSet());
		
		all_waiting.parallelStream().forEach(job -> {
			assertTrue(all_jobs_uuid.contains(job.getKey()));
		});
		
		store.checkConsistency();
		
		/**
		 * Test bulk simple update
		 */
		all_jobs.parallelStream().forEach(job -> {
			store.update(() -> {
				job.switchStatus(TaskStatus.POSTPONED);
				return job.getKey();
			});
		});
		
		store.checkConsistency();
		
		/**
		 * Test bulk update
		 */
		store.computeAndUpdate(TaskStatus.POSTPONED, (stream, uuid_resolver) -> {
			return stream.limit(10);
		}, job -> {
			job.switchStatus(TaskStatus.WAITING);
		});
		
		assertEquals(10, store.waitingJobCount());
		
		store.checkConsistency();
		
		/**
		 * Test bulk read
		 */
		assertEquals(10, store.getFromJobs(TaskStatus.WAITING, stream -> stream.collect(Collectors.toList())).size());
		
		/**
		 * Test bulk remove
		 */
		store.computeAndRemove(TaskStatus.POSTPONED, stream -> {
			return stream.limit(10);
		});
		
		assertEquals(all_jobs.size() - 10, store.size());
		assertEquals(10, store.waitingJobCount());
		
		store.checkConsistency();
	}
	
	public void testRandomPushPull() {
		List<Job> all_jobs = IntStream.range(0, 1_000).mapToObj(i -> JobUtilityTest.createJob("Test", "Test", new JsonObject(), null)).collect(Collectors.toList());
		List<Job> all_shuffle_jobs = new ArrayList<>(all_jobs);
		Collections.shuffle(all_shuffle_jobs);
		
		InMemoryJobStore store = new InMemoryJobStore();
		
		Thread t_push = new Thread(() -> {
			all_shuffle_jobs.stream().forEach(job -> {
				store.put(job);
			});
			System.out.println("End: " + Thread.currentThread().getName());
		}, "Push");
		
		Thread t_update = new Thread(() -> {
			all_jobs.stream().forEach(job -> {
				while (store.getByUUID(job.getKey()) == null) {
					Thread.onSpinWait();
				}
				job.switchStatus(TaskStatus.POSTPONED);
				store.update(() -> {
					return job.getKey();
				});
			});
			System.out.println("End: " + Thread.currentThread().getName());
		}, "Update");
		
		Thread t_pull = new Thread(() -> {
			while (store.getFromJobs(TaskStatus.POSTPONED, stream -> {
				return (int) stream.count();
			}) != all_jobs.size()) {
				Thread.onSpinWait();
			}
			System.out.println("End: " + Thread.currentThread().getName());
		}, "Pull");
		
		t_push.start();
		t_update.start();
		t_pull.start();
		
		while (t_push.isAlive() | t_update.isAlive() | t_pull.isAlive()) {
			Thread.onSpinWait();
		}
		
	}
	
}
