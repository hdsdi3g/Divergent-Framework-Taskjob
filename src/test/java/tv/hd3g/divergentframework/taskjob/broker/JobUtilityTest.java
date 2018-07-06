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
package tv.hd3g.divergentframework.taskjob.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

import com.google.gson.JsonObject;

import tv.hd3g.divergentframework.taskjob.broker.Broker;
import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.broker.TaskStatus;

public class JobUtilityTest {
	
	public static Job createJob(String description, String context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
		Job j = new Job();
		j.init(description, context_type, context_content, context_requirement_tags);
		return j;
	}
	
	/**
	 * @return job
	 */
	public static Job setExternalReference(Job job, String external_reference) {
		job.setExternalReference(external_reference);
		return job;
	}
	
	/**
	 * @return job
	 */
	public static Job setLinkedJob(Job job, UUID linked_job) {
		job.setLinkedJob(linked_job);
		return job;
	}
	
	public static UUID getLinkedJob(Job job) {
		return job.getLinkedJob();
	}
	
	/**
	 * @return job
	 */
	public static Job updateProgression(Job job, int actual_progression_value, int max_progression_value) {
		job.updateProgression(actual_progression_value, max_progression_value);
		return job;
	}
	
	/**
	 * @return sub job
	 */
	public static Job addSubJob(Job job, String description, String context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
		return job.addSubJob(description, context_type, context_content, context_requirement_tags);
	}
	
	/**
	 * @return job
	 */
	public static Job switchToError(Job job, Throwable e) {
		job.switchToError(e);
		return job;
	}
	
	/**
	 * @return job
	 */
	public static Job switchStatus(Job job, TaskStatus new_status) {
		job.switchStatus(new_status);
		return job;
	}
	
	/**
	 * @return job
	 */
	public static Job addPropertyInContext(Job job, String key, boolean value) {
		JsonObject jo = job.getContextContent();
		jo.addProperty(key, value);
		return job.setContextContent(jo);
	}
	
	/**
	 * @return job
	 */
	public static Job addPropertyInContext(Job job, String key, Number value) {
		JsonObject jo = job.getContextContent();
		jo.addProperty(key, value);
		return job.setContextContent(jo);
	}
	
	/**
	 * @return job
	 */
	public static Job addPropertyInContext(Job job, String key, String value) {
		JsonObject jo = job.getContextContent();
		jo.addProperty(key, value);
		return job.setContextContent(jo);
	}
	
	public final static Broker broker = new Broker() {
		
		public void updateProgression(Job action, int actual_value, int max_value) {
			throw new RuntimeException("Not implemented");
		}
		
		public void switchToError(Job job, Throwable e) {
			JobUtilityTest.switchToError(job, e);
		}
		
		public void switchStatus(Job job, TaskStatus new_status) {
			JobUtilityTest.switchStatus(job, new_status);
		}
		
		public void registerCallbackOnNewLocalJobsActivity(Runnable callback) {
			throw new RuntimeException("Not implemented");
		}
		
		public void getNextJobs(List<String> list_to_context_types, IntSupplier queue_capacity, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundActionReadyToStart) {
			throw new RuntimeException("Not implemented");
		}
		
		public List<Job> getJobsByUUID(List<UUID> keys) {
			throw new RuntimeException("Not implemented");
		}
		
		public List<Job> getAllJobs() {
			throw new RuntimeException("Not implemented");
		}
		
		public Job createJob(String description, String external_reference, String context_type, JsonObject context_content, Collection<String> context_requirement_tags) {
			throw new RuntimeException("Not implemented");
		}
		
		public Job addSubJob(Job reference, String description, String external_reference, String context_type, JsonObject context_content, Collection<String> context_requirement_tags) {
			throw new RuntimeException("Not implemented");
		}
		
	};
	
}
