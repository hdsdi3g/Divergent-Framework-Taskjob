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
package tv.hd3g.divergentframework.taskjob.events;

import java.awt.Color;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;

import junit.framework.TestCase;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.worker.Engine;

public class TestSimpleEventObserver extends TestCase {
	
	static class Dog {
		Color color;
		int size;
		
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (color == null ? 0 : color.hashCode());
			result = prime * result + size;
			return result;
		}
		
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Dog other = (Dog) obj;
			if (color == null) {
				if (other.color != null) {
					return false;
				}
			} else if (!color.equals(other.color)) {
				return false;
			}
			if (size != other.size) {
				return false;
			}
			return true;
		}
		
	}
	
	private class TestEngineObserver implements EngineEventObserver {
		
		Engine reference;
		final AtomicInteger change_ccrt_counter = new AtomicInteger(0);
		final AtomicInteger stop_counter = new AtomicInteger(0);
		final AtomicInteger start_counter = new AtomicInteger(0);
		final AtomicInteger ends_counter = new AtomicInteger(0);
		
		private void setReference(Engine reference) {
			this.reference = reference;
		}
		
		public void onEngineChangeContextRequirementTags(Engine engine) {
			assertEquals(reference, engine);
			change_ccrt_counter.incrementAndGet();
		}
		
		public void onEngineStop(Engine engine) {
			assertEquals(reference, engine);
			stop_counter.incrementAndGet();
		}
		
		public void onEngineStartProcess(Engine engine, Job job) {
			assertEquals(reference, engine);
			start_counter.incrementAndGet();
		}
		
		public void onEngineEndsProcess(Engine engine, Job job) {
			assertEquals(reference, engine);
			ends_counter.incrementAndGet();
		}
		
	}
	
	private class TestJobObserver implements JobEventObserver {
		
		HashSet<Job> references = new HashSet<>();
		final AtomicInteger init_counter = new AtomicInteger(0);
		final AtomicInteger update_counter = new AtomicInteger(0);
		final AtomicInteger progr_counter = new AtomicInteger(0);
		final AtomicInteger subjob_counter = new AtomicInteger(0);
		
		private void setReference(Job reference) {
			references.add(reference);
		}
		
		public void onJobAfterInit(Job job) {
			init_counter.incrementAndGet();
		}
		
		public void onJobUpdate(Job job, JobUpdateSubject cause) {
			update_counter.incrementAndGet();
		}
		
		public void onJobUpdateProgression(Job job) {
			assertTrue(references.contains(job));
			assertEquals(100, job.getMaxProgressionValue());
			progr_counter.incrementAndGet();
		}
		
		public void onJobAddSubJob(Job job, Job sub_job) {
			assertTrue(references.contains(job));
			subjob_counter.incrementAndGet();
		}
		
	}
	
	/**
	 * Based on TestLocalTaskJob
	 */
	public void test() throws InterruptedException {
		Gson gson = new Gson();
		
		Dog dogo = new Dog();
		dogo.color = Color.ORANGE;
		dogo.size = 5;
		
		ArrayList<Dog> captured_dogs = new ArrayList<>(1);
		ArrayList<Engine> engines = new ArrayList<>();
		HashMap<UUID, Job> jobs = new HashMap<>();
		
		InMemoryLocalTaskJob task_job = new InMemoryLocalTaskJob(10, 1, 1, 1, TimeUnit.SECONDS, jobs, engines);
		
		TestEngineObserver engine_observer = new TestEngineObserver();
		task_job.setEngineObserver(engine_observer);
		
		TestJobObserver job_observer = new TestJobObserver();
		task_job.setJobObserver(job_observer);
		
		assertEquals(0, engines.size());
		assertEquals(0, jobs.size());
		
		task_job.registerGenericEngine(1, "DogEngine", gson, Dog.class, () -> {
			return (referer, context, broker, shouldStopProcessing) -> {
				captured_dogs.add(context);
				broker.updateProgression(referer, 20, 100);
				broker.updateProgression(referer, 100, 100);
			};
		});
		
		assertEquals(1, engines.size());
		engine_observer.setReference(engines.get(0));
		
		Job job = task_job.createGenericJob("D", "", dogo, null, gson);
		
		assertEquals(1, jobs.size());
		job_observer.setReference(job);
		
		assertEquals(dogo, gson.fromJson(job.getContextContent(), Dog.class));
		
		while (captured_dogs.isEmpty()) {
			Thread.onSpinWait();
		}
		assertEquals(dogo, captured_dogs.get(0));
		
		assertEquals(0, engine_observer.change_ccrt_counter.get());
		assertEquals(0, engine_observer.stop_counter.get());
		assertEquals(1, engine_observer.start_counter.get());
		
		engine_observer.reference.setContextRequirementTags(Arrays.asList("SomeChanges"));
		assertEquals(1, engine_observer.change_ccrt_counter.get());
		
		while (engine_observer.ends_counter.get() == 0) {
			Thread.onSpinWait();
		}
		assertEquals(1, engine_observer.ends_counter.get());
		
		assertEquals(1, job_observer.init_counter.get());
		
		/**
		 * EXTERNAL_REFERENCE + (create job) + 2*SWITCH_STATUS + (start) + 2*SWITCH_STATUS
		 */
		assertEquals(5, job_observer.update_counter.get());
		assertEquals(2, job_observer.progr_counter.get());
		assertEquals(0, job_observer.subjob_counter.get());
		
		Job sub_job = task_job.createGenericSubJob(job, "NOPE", "", new Dog(), Arrays.asList("NEVER"), gson);
		
		assertFalse(sub_job.equals(job));
		assertEquals(2, job_observer.init_counter.get());
		
		/**
		 * ++EXTERNAL_REFERENCE
		 */
		assertEquals(6, job_observer.update_counter.get());
		assertEquals(2, job_observer.progr_counter.get());
		assertEquals(1, job_observer.subjob_counter.get());
		
	}
	
}
