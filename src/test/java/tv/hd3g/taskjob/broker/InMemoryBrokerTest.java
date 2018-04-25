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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.JsonObject;

import junit.framework.TestCase;

public class InMemoryBrokerTest extends TestCase {
	
	public void testCreateGetDone() throws Exception {
		long before_create = System.currentTimeMillis();
		
		InMemoryBroker broker = new InMemoryBroker(10, 1, 1, 1, TimeUnit.SECONDS);
		
		AtomicInteger callback_count = new AtomicInteger(0);
		broker.registerCallbackOnNewLocalJobsActivity(() -> {
			callback_count.getAndIncrement();
		});
		
		Job job = broker.createJob("D", "ER", "context", new JsonObject(), null);
		assertNotNull(job);
		
		assertEquals("D", job.getDescription());
		assertEquals("ER", job.getExternalReference());
		assertEquals("context", job.getContextType());
		assertEquals(0, job.getContextRequirementTags().size());
		assertFalse(job.hasContextRequirementTags());
		assertEquals(0, job.getContextContent().size());
		
		Thread.sleep(1);
		assertEquals(1, callback_count.get());
		
		assertTrue(before_create < job.getCreateDate());
		assertTrue(job.getCreateDate() < System.currentTimeMillis());
		assertEquals(0, job.getStartDate());
		assertEquals(0, job.getEndDate());
		long create_date = job.getCreateDate();
		
		List<Job> search = broker.getJobsByUUID(Arrays.asList(job.getKey()));
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		assertEquals(TaskStatus.WAITING, job.getStatus());
		
		AtomicInteger search_count = new AtomicInteger(0);
		
		broker.getNextJobs(Arrays.asList("context"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			assertEquals(job, selected_job);
			search_count.incrementAndGet();
			return true;
		});
		
		assertEquals(1, search_count.get());
		assertEquals(TaskStatus.PREPARING, job.getStatus());
		
		assertEquals(create_date, job.getCreateDate());
		assertEquals(0, job.getStartDate());
		assertEquals(0, job.getEndDate());
		
		broker.switchStatus(job, TaskStatus.PROCESSING);
		assertEquals(TaskStatus.PROCESSING, job.getStatus());
		
		assertEquals(create_date, job.getCreateDate());
		assertTrue(job.getStartDate() <= System.currentTimeMillis());
		assertEquals(0, job.getEndDate());
		long start_date = job.getStartDate();
		
		assertEquals(0, job.getActualProgressionValue());
		assertEquals(0, job.getMaxProgressionValue());
		broker.updateProgression(job, 5, 10);
		assertEquals(5, job.getActualProgressionValue());
		assertEquals(10, job.getMaxProgressionValue());
		
		broker.switchToError(job, new Exception("This is an error"));
		assertEquals(TaskStatus.ERROR, job.getStatus());
		assertEquals("This is an error", job.getLastErrorMessage());
		
		assertEquals(create_date, job.getCreateDate());
		assertEquals(start_date, job.getStartDate());
		assertTrue(job.getEndDate() <= System.currentTimeMillis());
		
		/*
		broker.addSubJob(reference, description, external_reference, context_type, context_content, context_requirement_tags);
		broker.flush();
		Test getNextJobs check queue capacity
		Test getNextJobs specific tags
		Test getNextJobs ignore other contexts
		
		Test push multiple jobs, with multiple registerCallbackOnNewLocalJobsActivity, with multiple parallel getNextJobs
		Test perfs big push and big flush
		*/
	}
	
}
