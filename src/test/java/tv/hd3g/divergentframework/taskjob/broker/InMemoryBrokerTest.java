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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import com.google.gson.JsonObject;

import junit.framework.TestCase;

public class InMemoryBrokerTest extends TestCase {
	
	static {
		Configurator.setLevel(InMemoryBroker.class.getName(), Level.WARN);
	}
	
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
		
		Thread.sleep(10);
		assertEquals(1, callback_count.get());
		
		assertTrue(before_create <= job.getCreateDate());
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
	}
	
	public void testSubJob() throws Exception {
		InMemoryBroker broker = new InMemoryBroker(10, 1, 1, 1, TimeUnit.SECONDS);
		
		Job job = broker.createJob("D1", "ER1", "context1", new JsonObject(), null);
		Job sub_job = broker.addSubJob(job, "D2", "ER2", "context2", new JsonObject(), null);
		
		assertNotNull(sub_job);
		assertEquals("D2", sub_job.getDescription());
		assertEquals("ER2", sub_job.getExternalReference());
		assertEquals("context2", sub_job.getContextType());
		assertEquals(0, sub_job.getContextRequirementTags().size());
		assertFalse(sub_job.hasContextRequirementTags());
		assertEquals(0, sub_job.getContextContent().size());
		
		List<Job> search = broker.getJobsByUUID(Arrays.asList(job.getKey(), sub_job.getKey()));
		assertNotNull(search);
		assertEquals(2, search.size());
		assertTrue(search.contains(job));
		assertTrue(search.contains(sub_job));
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(2, search.size());
		assertTrue(search.contains(job));
		assertTrue(search.contains(sub_job));
		
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		
		AtomicInteger search_count = new AtomicInteger(0);
		
		broker.getNextJobs(Arrays.asList("context1", "context2"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			assertEquals(job, selected_job);
			search_count.incrementAndGet();
			return true;
		});
		
		assertEquals(1, search_count.get());
		
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		broker.switchStatus(job, TaskStatus.PROCESSING);
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		
		search_count.set(0);
		broker.getNextJobs(Arrays.asList("context1", "context2"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			search_count.incrementAndGet();
			throw new RuntimeException("This should not be triggered");
		});
		
		assertEquals(0, search_count.get());
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		assertEquals(TaskStatus.PROCESSING, job.getStatus());
		
		broker.switchStatus(job, TaskStatus.ERROR);
		
		broker.getNextJobs(Arrays.asList("context1", "context2"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			throw new RuntimeException("This should not be triggered");
		});
		
		broker.switchStatus(job, TaskStatus.WAITING);
		
		broker.getNextJobs(Arrays.asList("context1", "context2"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			assertEquals(job, selected_job);
			return true;
		});
		
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		broker.switchStatus(job, TaskStatus.PROCESSING);
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		broker.switchStatus(job, TaskStatus.DONE);
		assertEquals(TaskStatus.WAITING, sub_job.getStatus());
		
		broker.getNextJobs(Arrays.asList("context1", "context2"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			assertEquals(sub_job, selected_job);
			return true;
		});
		
		assertEquals(TaskStatus.PREPARING, sub_job.getStatus());
		broker.switchStatus(sub_job, TaskStatus.PROCESSING);
	}
	
	public void testFlushAbandonedJobs() throws Exception {
		long wait_duration = 100;
		
		InMemoryBroker broker = new InMemoryBroker(10, wait_duration, 100_000 * wait_duration, 100_000 * wait_duration, TimeUnit.MILLISECONDS);
		Job job = broker.createJob("D1", "ER1", "context1", new JsonObject(), null);
		
		List<Job> search = broker.getJobsByUUID(Arrays.asList(job.getKey()));
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		broker.flush();
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		Thread.sleep(wait_duration + 10);
		
		broker.flush();
		
		search = broker.getJobsByUUID(Arrays.asList(job.getKey()));
		assertNotNull(search);
		assertTrue(search.isEmpty());
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertTrue(search.isEmpty());
	}
	
	public void testFlushDoneJobs() throws Exception {
		long wait_duration = 100;
		
		InMemoryBroker broker = new InMemoryBroker(10, 100_000 * wait_duration, wait_duration, 100_000 * wait_duration, TimeUnit.MILLISECONDS);
		Job job = broker.createJob("D1", "ER1", "context1", new JsonObject(), null);
		
		Thread.sleep(wait_duration + 10);
		broker.flush();
		
		List<Job> search = broker.getJobsByUUID(Arrays.asList(job.getKey()));
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		broker.getNextJobs(Arrays.asList("context1"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			assertEquals(job, selected_job);
			return true;
		});
		
		job.switchStatus(TaskStatus.PROCESSING);
		
		Thread.sleep(wait_duration + 10);
		broker.flush();
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		job.switchStatus(TaskStatus.DONE);
		
		broker.flush();
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		Thread.sleep(wait_duration + 10);
		broker.flush();
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertTrue(search.isEmpty());
	}
	
	public void testFlushErrorJobs() throws Exception {
		long wait_duration = 100;
		
		InMemoryBroker broker = new InMemoryBroker(10, 100_000 * wait_duration, 100_000 * wait_duration, wait_duration, TimeUnit.MILLISECONDS);
		Job job = broker.createJob("D1", "ER1", "context1", new JsonObject(), null);
		
		Thread.sleep(wait_duration + 10);
		broker.flush();
		
		List<Job> search = broker.getJobsByUUID(Arrays.asList(job.getKey()));
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		broker.getNextJobs(Arrays.asList("context1"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			throw new RuntimeException("This should not be triggered");
		}, selected_job -> {
			assertEquals(job, selected_job);
			return true;
		});
		
		job.switchStatus(TaskStatus.PROCESSING);
		job.switchStatus(TaskStatus.ERROR);
		
		broker.flush();
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertEquals(1, search.size());
		assertEquals(job, search.get(0));
		
		Thread.sleep(wait_duration + 10);
		broker.flush();
		
		search = broker.getAllJobs();
		assertNotNull(search);
		assertTrue(search.isEmpty());
	}
	
	public void testCapacity() throws Exception {
		InMemoryBroker broker = new InMemoryBroker(10, 1, 1, 1, TimeUnit.SECONDS);
		
		IntStream.range(0, 10).forEach(i -> {
			assertNotNull(broker.createJob("D", "ER", "context", new JsonObject(), null));
		});
		
		FullJobStoreException fjse = null;
		try {
			broker.createJob("D", "ER", "context", new JsonObject(), null);
		} catch (FullJobStoreException e) {
			fjse = e;
		}
		assertNotNull(fjse);
	}
	
	public void testSpecificTagsAndContext() throws Exception {
		InMemoryBroker broker = new InMemoryBroker(10, 1, 1, 1, TimeUnit.SECONDS);
		
		/*Job job_bad_context =*/ broker.createJob("D", "ER", "contextNOPE", new JsonObject(), null);
		Job job_wo_tag = broker.createJob("D", "ER", "context", new JsonObject(), null);
		Job job_w_tag = broker.createJob("D", "ER", "context", new JsonObject(), Arrays.asList("tag"));
		/*Job job_bad_tag =*/ broker.createJob("D", "ER", "context", new JsonObject(), Arrays.asList("tNOPE"));
		Job job_w_2tags = broker.createJob("D", "ER", "context", new JsonObject(), Arrays.asList("tag1", "tag2"));
		/*Job job_bad_1tag =*/ broker.createJob("D", "ER", "context", new JsonObject(), Arrays.asList("tag", "tNOPE"));
		
		List<String> ok_list = Arrays.asList("tag", "tag1", "tag2");
		
		final ArrayList<Job> validated = new ArrayList<>();
		
		broker.getNextJobs(Arrays.asList("context"), () -> {
			return 10;
		}, (c_type, c_tags) -> {
			assertEquals(c_type, "context");
			return ok_list.containsAll(c_tags);
		}, selected_job -> {
			validated.add(selected_job);
			return true;
		});
		
		assertEquals(3, validated.size());
		assertTrue(validated.contains(job_wo_tag));
		assertTrue(validated.contains(job_w_tag));
		assertTrue(validated.contains(job_w_2tags));
	}
	
	public void testParallel() throws Exception {
		
		InMemoryBroker broker = new InMemoryBroker(100_000, 1, 1, 1, TimeUnit.SECONDS);
		
		final int CPU_COUNT = Math.max(Runtime.getRuntime().availableProcessors(), 2);
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(CPU_COUNT, CPU_COUNT, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		
		Runnable onNewJobQueue1 = () -> {
			broker.getNextJobs(Arrays.asList("context0"), () -> {
				return 1;
			}, (c_type, c_tags) -> {
				throw new RuntimeException("This should not be triggered");
			}, selected_job -> {
				executor.execute(() -> {
					if (selected_job.getContextType().equals("context0") == false) {
						throw new RuntimeException("Invalid context: " + selected_job.getContextType() + " for " + selected_job.getDescription());
					}
					broker.switchStatus(selected_job, TaskStatus.PROCESSING);
					broker.updateProgression(selected_job, 5, 10);
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
					}
					broker.switchStatus(selected_job, TaskStatus.DONE);
				});
				return true;
			});
		};
		
		Runnable onNewJobQueue2 = () -> {
			broker.getNextJobs(Arrays.asList("context1"), () -> {
				return 1;
			}, (c_type, c_tags) -> {
				throw new RuntimeException("This should not be triggered");
			}, selected_job -> {
				executor.execute(() -> {
					if (selected_job.getContextType().equals("context1") == false) {
						throw new RuntimeException("Invalid context: " + selected_job.getContextType() + " for " + selected_job.getDescription());
					}
					broker.switchStatus(selected_job, TaskStatus.PROCESSING);
					broker.updateProgression(selected_job, 5, 10);
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
					}
					broker.switchStatus(selected_job, TaskStatus.DONE);
				});
				return true;
			});
		};
		
		broker.registerCallbackOnNewLocalJobsActivity(() -> executor.execute(onNewJobQueue1));
		broker.registerCallbackOnNewLocalJobsActivity(() -> executor.execute(onNewJobQueue2));
		
		IntStream.range(0, 600).parallel().forEach(i -> {
			assertNotNull(broker.createJob("D-" + i, "#" + i, "context" + i % 2, new JsonObject(), null));
		});
		
		assertEquals(600, broker.getAllJobs().size());
		
		while (broker.getAllJobs().stream().allMatch(job -> {
			return job.getStatus().equals(TaskStatus.DONE);
		}) == false) {
			Thread.onSpinWait();
		}
		
		broker.getAllJobs().parallelStream().forEach(job -> {
			if (job.getStatus().equals(TaskStatus.DONE) == false) {
				throw new RuntimeException("Invalid job status: " + job.getDescription() + "/" + job.getContextType() + "/" + job.getStatus());
			} else if (job.getActualProgressionValue() != 5) {
				throw new RuntimeException("Invalid job ProgressionValue: " + job.getDescription() + "/" + job.getContextType() + "/" + job.getActualProgressionValue());
			}
		});
		
	}
	
	/**
	 * 100k in 1,4 sec on a i7/2013
	 */
	public void testBigPushAndFlush() throws Exception {
		int count = 100_000;
		int slices = count / 10;
		
		InMemoryBroker broker = new InMemoryBroker(count * 2, 60_000, 1, 1, TimeUnit.MILLISECONDS);
		
		IntStream.range(0, count).parallel().forEach(i -> {
			assertNotNull(broker.createJob("D-" + i, "#" + i, "context" + i % 2, new JsonObject(), null));
		});
		
		IntStream.range(0, count / slices + 1).parallel().forEach(i -> {
			ArrayList<Job> jobs = new ArrayList<>(slices);
			
			broker.getNextJobs(Arrays.asList("context0"), () -> {
				return slices - jobs.size();
			}, (c_type, c_tags) -> {
				throw new RuntimeException("This should not be triggered");
			}, selected_job -> {
				return jobs.add(selected_job);
			});
			
			jobs.stream().parallel().forEach(selected_job -> {
				if (selected_job.getContextType().equals("context0") == false) {
					throw new RuntimeException("Invalid context: " + selected_job.getContextType() + " for " + selected_job.getDescription());
				}
				broker.switchStatus(selected_job, TaskStatus.PROCESSING);
				broker.switchStatus(selected_job, TaskStatus.DONE);
			});
			
			broker.flush();
		});
		
		Thread.sleep(2);
		broker.flush();
		assertEquals(count / 2, broker.getAllJobs().size());
		
		broker.getAllJobs().parallelStream().forEach(job -> {
			if (job.getContextType().equals("context0")) {
				throw new RuntimeException("Invalid job context0: " + job);
			}
		});
	}
	
}
