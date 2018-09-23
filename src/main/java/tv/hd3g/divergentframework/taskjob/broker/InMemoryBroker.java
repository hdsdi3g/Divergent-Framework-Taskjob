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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import tv.hd3g.divergentframework.taskjob.events.JobEventObserver;

/**
 * Store and retrieve jobs, localy in this instance, with JobStore.
 */
public class InMemoryBroker implements Broker {
	private static final Logger log = LogManager.getLogger();
	
	private int max_job_count;
	private long abandoned_jobs_retention_time;
	private long done_jobs_retention_time;
	private long error_jobs_retention_time;
	private final InMemoryJobStore store;
	private final ArrayList<Runnable> on_new_local_jobs_activity_callbacks;
	
	private final ThreadPoolExecutor executor;
	private final ScheduledThreadPoolExecutor sch_maintenance_exec;
	private final ScheduledFuture<?> cleanup_task;
	
	private JobEventObserver job_observer;
	
	public InMemoryBroker(int max_job_count, long abandoned_jobs_retention_time, long done_jobs_retention_time, long error_jobs_retention_time, TimeUnit unit, Map<UUID, Job> external_jobs_by_uuid) {
		this.max_job_count = max_job_count;
		this.abandoned_jobs_retention_time = unit.toMillis(abandoned_jobs_retention_time);
		this.done_jobs_retention_time = unit.toMillis(done_jobs_retention_time);
		this.error_jobs_retention_time = unit.toMillis(error_jobs_retention_time);
		on_new_local_jobs_activity_callbacks = new ArrayList<>(1);
		
		executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
			Thread t = new Thread(runnable);
			t.setName("BrokerExecutor");
			t.setPriority(Thread.MIN_PRIORITY);
			t.setDaemon(true);
			return t;
		});
		store = new InMemoryJobStore(external_jobs_by_uuid);
		
		long min_delay_to_update = Math.min(Math.min(abandoned_jobs_retention_time, done_jobs_retention_time), error_jobs_retention_time);
		log.debug("Set regular flush task every " + min_delay_to_update + " " + unit.name().toLowerCase());
		
		sch_maintenance_exec = new ScheduledThreadPoolExecutor(1, r -> {
			Thread t = new Thread(r);
			t.setDaemon(true);
			t.setPriority(Thread.MIN_PRIORITY);
			t.setName("RegularFlushTask");
			return t;
		});
		
		cleanup_task = sch_maintenance_exec.scheduleWithFixedDelay(() -> {
			if (storeSize() > 0) {
				executor.execute(() -> {
					log.info("Flush task list");
					flush();
				});
			}
		}, min_delay_to_update, min_delay_to_update, unit);
	}
	
	public InMemoryBroker(int max_job_count, long abandoned_jobs_retention_time, long done_jobs_retention_time, long error_jobs_retention_time, TimeUnit unit) {
		this(max_job_count, abandoned_jobs_retention_time, done_jobs_retention_time, error_jobs_retention_time, unit, new HashMap<>());
	}
	
	public Optional<RuntimeException> checkStoreConsistency() {
		return store.checkConsistency();
	}
	
	public InMemoryBroker cancelCleanUpTask() {
		cleanup_task.cancel(false);
		return this;
	}
	
	public int storeSize() {
		return store.size();
	}
	
	/**
	 * @return this
	 */
	public InMemoryBroker setJobObserver(JobEventObserver job_observer) {
		this.job_observer = job_observer;
		return this;
	}
	
	/**
	 * @return this
	 */
	public InMemoryBroker flush() {
		store.computeAllAndRemove((stream, uuid_resolver) -> {
			return stream.filter(job -> {
				if (job.getStatus().isDone()) {
					if (job.getStatus().equals(TaskStatus.DONE)) {
						/**
						 * Search all dependant linked job.
						 */
						List<UUID> relatives = job.getRelativesJobsUUID();
						if (relatives.isEmpty() == false) {
							if (relatives.stream().map(sub_job_uuid -> {
								return uuid_resolver.apply(sub_job_uuid);
							}).filter(sub_job -> sub_job != null).anyMatch(sub_job -> {
								return sub_job.getStatus().isDone() == false;
							})) {
								/**
								 * If some sub-tasks are not yet done, keep main task.
								 */
								return false;
							}
						}
						
						return job.isTooOld(done_jobs_retention_time);
					} else if (job.getStatus().equals(TaskStatus.ERROR)) {
						return job.isTooOld(error_jobs_retention_time);
					}
				} else if (job.getStatus().equals(TaskStatus.POSTPONED)) {
					/**
					 * Postponed task will not expire.
					 */
					return false;
				} else if (job.getStatus().equals(TaskStatus.WAITING)) {
					/**
					 * Search the dependant linked job.
					 */
					if (job.getLinkedJob() != null) {
						Job linked_job = uuid_resolver.apply(job.getLinkedJob());
						if (linked_job == null) {
							/**
							 * Main task was deleted... delete this sub task.
							 */
							return true;
						}
					}
				}
				return job.isTooOld(abandoned_jobs_retention_time);
			});
		});
		
		return this;
	}
	
	public List<Job> getJobsByUUID(List<UUID> keys) {
		if (keys == null) {
			return Collections.emptyList();
		}
		if (keys.isEmpty()) {
			return Collections.emptyList();
		}
		
		return keys.stream().map(uuid -> {
			return store.getByUUID(uuid);
		}).filter(job -> {
			return job != null;
		}).collect(Collectors.toList());
	}
	
	public List<Job> getAllJobs() {
		return store.getFromJobs(stream -> {
			return stream.collect(Collectors.toList());
		});
	}
	
	public void updateProgression(Job job, int actual_value, int max_value) {
		boolean ok = store.update(() -> {
			return job.updateProgression(actual_value, max_value).getKey();
		});
		if (ok == false) {
			log.warn("Can't update job " + job);
		}
	}
	
	public void switchToError(Job job, Throwable e) {
		boolean ok = store.update(() -> {
			return job.switchToError(e).getKey();
		});
		if (ok == false) {
			log.warn("Can't update job " + job);
		}
	}
	
	public void switchStatus(Job job, TaskStatus new_status) {
		boolean ok = store.update(() -> {
			return job.switchStatus(new_status).getKey();
		});
		if (ok == false) {
			log.warn("Can't update job " + job);
		}
	}
	
	public <T> Job createGenericJob(String description, String external_reference, T context, Collection<String> context_requirement_tags, Gson gson) {
		return createJob(description, external_reference, Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE + context.getClass().getName(), gson.toJsonTree(context).getAsJsonObject(), context_requirement_tags);
	}
	
	public Job createJob(String description, String external_reference, String context_type, JsonObject context_content, Collection<String> context_requirement_tags) {
		if (store.size() >= max_job_count) {
			throw new FullJobStoreException();
		}
		Job job = new Job();
		
		ArrayList<String> l_context_requirement_tags = null;
		if (context_requirement_tags != null) {
			l_context_requirement_tags = new ArrayList<>(context_requirement_tags);
		}
		job.init(description, context_type, context_content, l_context_requirement_tags, job_observer);
		job.setExternalReference(external_reference);
		
		if (store.put(job) == false) {
			throw new RuntimeException("Can't put job in internal store: " + job);
		}
		
		log.info("Create job " + job);
		
		on_new_local_jobs_activity_callbacks.stream().forEach(r -> {
			executor.execute(r);
		});
		
		return job;
	}
	
	public <T> Job createGenericSubJob(Job reference, String description, String external_reference, T context, Collection<String> context_requirement_tags, Gson gson) {
		return addSubJob(reference, description, external_reference, Job.JAVA_CLASS_PREFIX_CONTEXT_TYPE + context.getClass().getName(), gson.toJsonTree(context).getAsJsonObject(), context_requirement_tags);
	}
	
	public Job addSubJob(Job reference, String description, String external_reference, String context_type, JsonObject context_content, Collection<String> context_requirement_tags) {
		if (store.size() >= max_job_count) {
			throw new FullJobStoreException();
		}
		
		ArrayList<String> l_context_requirement_tags = null;
		if (context_requirement_tags != null) {
			l_context_requirement_tags = new ArrayList<>(context_requirement_tags);
		}
		
		Job sub_job = reference.addSubJob(description, context_type, context_content, l_context_requirement_tags).setExternalReference(external_reference);
		
		if (store.put(sub_job) == false) {
			throw new RuntimeException("Can't put sub_job in internal store: " + sub_job);
		}
		
		log.info("Create sub job " + sub_job + " referenced by " + reference);
		
		on_new_local_jobs_activity_callbacks.stream().forEach(r -> {
			executor.execute(r);
		});
		
		return sub_job;
	}
	
	public void registerCallbackOnNewLocalJobsActivity(Runnable callback) {
		synchronized (on_new_local_jobs_activity_callbacks) {
			on_new_local_jobs_activity_callbacks.add(callback);
		}
	}
	
	public void getNextJobs(List<String> list_to_context_types, IntSupplier queue_capacity, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundJobReadyToStart) {
		HashSet<String> set_context_types = new HashSet<>(list_to_context_types);
		
		List<Job> pre_selected_jobs = store.computeAndUpdate(TaskStatus.WAITING, (stream, job_by_uuid_resolver) -> {
			return stream.takeWhile(job -> {
				/**
				 * checkQueueCapacity
				 */
				return queue_capacity.getAsInt() > 0;
			}).filter(job -> {
				/**
				 * checkJobIsNotTooOld
				 */
				return job.isTooOld(abandoned_jobs_retention_time) == false;
			}).filter(job -> {
				/**
				 * checkJobContextType
				 */
				return set_context_types.contains(job.getContextType());
			}).filter(job -> {
				/**
				 * checkJobTagsByContextType
				 */
				if (job.hasContextRequirementTags() == false) {
					return true;
				}
				return filterByContextTypeAndTags.test(job.getContextType(), job.getContextRequirementTags());
			}).filter(job -> {
				/**
				 * Check linked_job status
				 */
				UUID linked_job_uuid = job.getLinkedJob();
				if (linked_job_uuid == null) {
					return true;
				}
				Job linked_job = job_by_uuid_resolver.apply(linked_job_uuid);
				if (linked_job == null) {
					/**
					 * linked_job deleted !
					 */
					return false;
				}
				
				return linked_job.getStatus() == TaskStatus.DONE;
			}).sorted((l, r) -> {
				return Long.compare(l.getCreateDate(), r.getCreateDate());
			});
		}, job -> {
			job.switchStatus(TaskStatus.PREPARING);
		});
		
		pre_selected_jobs.stream().filter(preparing_job -> {
			return onFoundJobReadyToStart.test(preparing_job) == false;
		}).forEach(rejected_preparing_job -> {
			boolean put_ok = store.update(() -> {
				/**
				 * Finally, this job can't to be process now. Re-switch to waiting.
				 */
				rejected_preparing_job.switchStatus(TaskStatus.WAITING);
				return rejected_preparing_job.getKey();
			});
			if (put_ok == false) {
				throw new RuntimeException("Can't put new job status " + rejected_preparing_job);
			}
		});
	}
	
}
