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
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;

import junit.framework.TestCase;
import tv.hd3g.divergentframework.taskjob.InMemoryLocalTaskJob;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;
import tv.hd3g.divergentframework.taskjob.worker.Engine;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

public class TestSimpleEventObserver extends TestCase {
	
	private static final Gson gson = new Gson();
	
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
		
		private Engine reference;
		private final AtomicInteger change_ccrt_counter = new AtomicInteger(0);
		private final AtomicInteger stop_counter = new AtomicInteger(0);
		private final AtomicInteger start_counter = new AtomicInteger(0);
		private final AtomicInteger ends_counter = new AtomicInteger(0);
		private final AtomicInteger reg_engine_counter = new AtomicInteger(0);
		private final AtomicInteger un_reg_engine_counter = new AtomicInteger(0);
		
		private void setReference(Engine reference) {
			this.reference = reference;
			if (reference == null) {
				throw new NullPointerException("\"reference\" can't to be null");
			}
		}
		
		public void onEngineChangeContextRequirementTags(Engine engine) {
			if (reference.equals(engine) == false) {
				throw new RuntimeException("Invalid engine: " + engine);
			}
			change_ccrt_counter.incrementAndGet();
		}
		
		public void onEngineStop(Engine engine) {
			if (reference.equals(engine) == false) {
				throw new RuntimeException("Invalid engine: " + engine);
			}
			stop_counter.incrementAndGet();
		}
		
		public void onEngineStartProcess(Engine engine, WorkerThread w_t) {
			if (reference.equals(engine) == false) {
				throw new RuntimeException("Invalid engine: " + engine);
			}
			start_counter.incrementAndGet();
		}
		
		public void onEngineEndsProcess(Engine engine, WorkerThread w_t) {
			if (reference.equals(engine) == false) {
				throw new RuntimeException("Invalid engine: " + engine);
			}
			ends_counter.incrementAndGet();
		}
		
		public void onRegisterEngine(Engine engine) {
			if (reference != null) {
				if (reference.equals(engine) == false) {
					throw new RuntimeException("Invalid engine: " + engine);
				}
			}
			reg_engine_counter.incrementAndGet();
		}
		
		public void onUnRegisterEngine(Engine engine) {
			if (reference.equals(engine) == false) {
				throw new RuntimeException("Invalid engine: " + engine);
			}
			un_reg_engine_counter.incrementAndGet();
		}
		
	}
	
	private class TestJobObserver implements JobEventObserver {
		
		HashSet<Job> references = new HashSet<>();
		final AtomicInteger update_counter = new AtomicInteger(0);
		final AtomicInteger progr_counter = new AtomicInteger(0);
		final AtomicInteger subjob_counter = new AtomicInteger(0);
		final AtomicInteger afterflush_counter = new AtomicInteger(0);
		final AtomicInteger createjob_counter = new AtomicInteger(0);
		
		private void setReference(Job reference) {
			references.add(reference);
		}
		
		public void onJobUpdate(Job job, JobUpdateSubject cause) {
			update_counter.incrementAndGet();
		}
		
		public void onJobUpdateProgression(Job job) {
			assertTrue(references.contains(job));
			assertEquals(100, job.getMaxProgressionValue());
			progr_counter.incrementAndGet();
		}
		
		public void brokerOnAfterFlush(List<UUID> deleted_jobs_uuid) {
			// assertTrue(references.stream().map(j -> j.getKey()).collect(Collectors.toUnmodifiableList()).containsAll(deleted_jobs_uuid));
			afterflush_counter.incrementAndGet();
		}
		
		public void brokerOnCreateJob(Job job) {
			assertNotNull(job);
			assertFalse(references.contains(job));
			createjob_counter.incrementAndGet();
		}
		
		public void brokerOnCreateSubJob(Job reference, Job sub_job) {
			assertTrue(references.contains(reference));
			subjob_counter.incrementAndGet();
		}
		
	}
	
	public void testAddRemoveEngine() {
		InMemoryLocalTaskJob task_job = new InMemoryLocalTaskJob(10, 1, 1, 1, TimeUnit.SECONDS);
		TestEngineObserver engine_observer = new TestEngineObserver();
		task_job.setEngineObserver(engine_observer);
		
		assertEquals(0, engine_observer.reg_engine_counter.get());
		assertEquals(0, engine_observer.un_reg_engine_counter.get());
		
		Engine engine = task_job.registerGenericEngine(1, "DogEngine", gson, Dog.class, () -> {
			return (referer, context, broker, shouldStopProcessing) -> {
			};
		});
		
		engine_observer.setReference(engine);
		assertEquals(1, engine_observer.reg_engine_counter.get());
		assertEquals(0, engine_observer.un_reg_engine_counter.get());
		
		task_job.unRegisterGenericEngine(Dog.class);
		
		assertEquals(1, engine_observer.reg_engine_counter.get());
		assertEquals(1, engine_observer.un_reg_engine_counter.get());
		
		task_job.registerEngine(engine);
		
		assertEquals(2, engine_observer.reg_engine_counter.get());
		assertEquals(1, engine_observer.un_reg_engine_counter.get());
		
		task_job.unRegisterEngine(engine);
		
		assertEquals(2, engine_observer.reg_engine_counter.get());
		assertEquals(2, engine_observer.un_reg_engine_counter.get());
	}
	
	/**
	 * Based on TestLocalTaskJob
	 */
	public void test() throws InterruptedException {
		Dog dogo = new Dog();
		dogo.color = Color.ORANGE;
		dogo.size = 5;
		
		ArrayList<Dog> captured_dogs = new ArrayList<>(1);
		
		InMemoryLocalTaskJob task_job = new InMemoryLocalTaskJob(10, 500, 500, 500, TimeUnit.MILLISECONDS);
		
		TestEngineObserver engine_observer = new TestEngineObserver();
		TestJobObserver job_observer = new TestJobObserver();
		
		task_job.setEngineObserver(engine_observer);
		task_job.setJobObserver(job_observer);
		
		engine_observer.setReference(task_job.registerGenericEngine(1, "DogEngine", gson, Dog.class, () -> {
			return (referer, context, broker, shouldStopProcessing) -> {
				captured_dogs.add(context);
				broker.updateProgression(referer, 20, 100);
				broker.updateProgression(referer, 100, 100);
			};
		}));
		
		assertEquals(0, job_observer.createjob_counter.get());
		
		Job job = task_job.createGenericJob("D", "", dogo, null, gson);
		
		job_observer.setReference(job);
		
		assertEquals(1, job_observer.createjob_counter.get());
		
		assertEquals(dogo, gson.fromJson(job.getContextContent(), Dog.class));
		
		while (captured_dogs.isEmpty()) {
			Thread.onSpinWait();
		}
		assertEquals(dogo, captured_dogs.get(0));
		
		while (job.getStatus() != TaskStatus.DONE) {
			Thread.onSpinWait();
		}
		
		assertEquals(0, engine_observer.change_ccrt_counter.get());
		assertEquals(0, engine_observer.stop_counter.get());
		assertEquals(1, engine_observer.start_counter.get());
		
		engine_observer.reference.setContextRequirementTags(Arrays.asList("SomeChanges"));
		assertEquals(1, engine_observer.change_ccrt_counter.get());
		
		while (engine_observer.ends_counter.get() == 0) {
			Thread.onSpinWait();
		}
		assertEquals(1, engine_observer.ends_counter.get());
		
		/**
		 * EXTERNAL_REFERENCE + (create job) + 2*SWITCH_STATUS + (start) + 2*SWITCH_STATUS
		 */
		assertEquals(4, job_observer.update_counter.get());
		assertEquals(2, job_observer.progr_counter.get());
		assertEquals(0, job_observer.subjob_counter.get());
		
		Job sub_job = task_job.createGenericSubJob(job, "NOPE", "", new Dog(), Arrays.asList("NEVER"), gson);
		
		assertFalse(sub_job.equals(job));
		assertEquals(1, job_observer.createjob_counter.get());
		assertEquals(1, job_observer.subjob_counter.get());
		
		/**
		 * ++EXTERNAL_REFERENCE
		 */
		// FIXME assertEquals(4, job_observer.update_counter.get());
		assertEquals(2, job_observer.progr_counter.get());
		assertEquals(1, job_observer.subjob_counter.get());
		
		task_job.checkStoreConsistency().ifPresent(e -> {
			throw e;
		});
		
		assertEquals(0, job_observer.afterflush_counter.get());
		
		while (job_observer.afterflush_counter.get() == 0) {
			Thread.onSpinWait();
		}
	}
	
}
