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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

public class InMemoryBroker implements Broker {
	private static Logger log = Logger.getLogger(InMemoryBroker.class);
	
	private int max_job_count;
	private long abandoned_jobs_retention_time;
	private long done_jobs_retention_time;
	private long error_jobs_retention_time;
	private final JobStore store;
	
	public InMemoryBroker(int max_job_count, long abandoned_jobs_retention_time, long done_jobs_retention_time, long error_jobs_retention_time, TimeUnit unit) {
		this.max_job_count = max_job_count;
		this.abandoned_jobs_retention_time = unit.toMillis(abandoned_jobs_retention_time);
		this.done_jobs_retention_time = unit.toMillis(done_jobs_retention_time);
		this.error_jobs_retention_time = unit.toMillis(error_jobs_retention_time);
		store = new JobStore();
	}
	
	/**
	 * @return this
	 */
	InMemoryBroker flush() {
		store.computeAllAndRemove((stream, uuid_resolver) -> {
			return stream.filter(job -> {
				if (job.getStatus().isDone()) {
					if (job.getStatus().equals(TaskStatus.DONE)) {
						/**
						 * Search the dependant linked job.
						 */
						if (job.getLinkedJob() != null) {
							Job linked_job = uuid_resolver.apply(job.getLinkedJob());
							if (linked_job != null) {
								if (linked_job.getStatus().isDone() == false) {
									/**
									 * Protect sub-tasks if main task is not yet processed.
									 */
									return false;
								}
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
	
	@Override
	public Job createJob(String description, String external_reference, JsonObject context, ArrayList<String> context_requirement_tags) {
		// max_job_count
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Job addSubJob(Job reference, String description, JsonObject context, ArrayList<String> context_requirement_tags) {
		// max_job_count
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void onNewLocalJobsActivity(Runnable callback) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void getNextActions(List<String> list_to_context_types, IntSupplier queue_capacity, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundActionReadyToStart) {
		// TODO Auto-generated method stub
		
	}
	
}
