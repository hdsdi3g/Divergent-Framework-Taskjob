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
package tv.hd3g.taskjob.broker;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.gson.JsonObject;

import junit.framework.TestCase;

public class TestJobStore extends TestCase {
	
	public void testPushPull() {
		JobStore store = new JobStore();
		
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
	}
	
	public void testParallelPushPull() {
		List<Job> all_jobs = IntStream.range(0, 100_000).mapToObj(i -> JobUtilityTest.createJob("Test", "Test", new JsonObject(), null)).collect(Collectors.toList());
		
		JobStore store = new JobStore();
		
		all_jobs.parallelStream().forEach(job -> {
			assertTrue(store.put(job));
		});
		
		assertEquals(all_jobs.size(), store.size());
		
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
	}
	
}
