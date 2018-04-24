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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

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
		// TODO Auto-generated method stub
		return this;
	}
	
	@Override
	public List<Job> getJobsByUUID(Job job, List<UUID> keys) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Job createJob(String description, String external_reference, JsonObject context, ArrayList<String> context_requirement_tags) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Job addSubJob(Job reference, String description, JsonObject context, ArrayList<String> context_requirement_tags) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public List<Job> getAllJobs() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void updateProgression(Job action, int actual_value, int max_value) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void switchToError(Job job, Throwable e) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void switchStatus(Job job, TaskStatus new_status) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void getNextActions(List<String> list_to_context_types, IntSupplier queue_capacity, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundActionReadyToStart) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onNewLocalJobsActivity(Runnable callback) {
		// TODO Auto-generated method stub
		
	}
	
}
