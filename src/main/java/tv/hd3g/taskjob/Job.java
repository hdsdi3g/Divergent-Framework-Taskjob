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
package tv.hd3g.taskjob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.google.gson.JsonObject;

import tv.hd3g.taskjob.broker.Broker;

public final class Job {
	private static Logger log = Logger.getLogger(Job.class);
	
	private long create_date;
	private String description;
	private UUID key;
	
	private String context_type;
	private JsonObject context_content;
	private ArrayList<String> context_requirement_tags;
	
	private TaskStatus status;
	private long start_date;
	private long end_date;
	
	private ArrayList<UUID> relatives_sub_jobs;
	private UUID linked_job;
	
	private String last_error_message;
	
	private String external_reference;
	private int actual_progression_value;
	private int max_progression_value;
	
	UUID init(String description, Class<?> context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
		this.description = description;
		
		key = UUID.randomUUID();
		
		this.context_type = context_type.getName();
		this.context_content = context_content;
		this.context_requirement_tags = context_requirement_tags;
		
		status = TaskStatus.WAITING;
		start_date = 0;
		end_date = 0;
		relatives_sub_jobs = new ArrayList<>();
		
		return key;
	}
	
	/**
	 * @return this
	 */
	Job setExternalReference(String external_reference) {
		this.external_reference = external_reference;
		return this;
	}
	
	/**
	 * @return this
	 */
	Job setLinkedJob(UUID linked_job) {
		this.linked_job = linked_job;
		return this;
	}
	
	UUID getLinkedJob() {
		return linked_job;
	}
	
	public UUID getKey() {
		return key;
	}
	
	/**
	 * @return this
	 */
	Job updateProgression(int actual_progression_value, int max_progression_value) {
		this.actual_progression_value = actual_progression_value;
		this.max_progression_value = max_progression_value;
		return this;
	}
	
	public List<Job> getRelativesJobs(Broker broker) {
		if (relatives_sub_jobs == null) {
			return Collections.emptyList();
		}
		return broker.getJobsByUUID(this, relatives_sub_jobs);
	}
	
	Job addSubJob(String description, Class<?> context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
		Job sub_job = new Job();
		UUID new_uuid = sub_job.setLinkedJob(key).init(description, context_type, context_content, context_requirement_tags);
		
		synchronized (this) {
			if (relatives_sub_jobs == null) {
				relatives_sub_jobs = new ArrayList<>();
			}
		}
		
		synchronized (relatives_sub_jobs) {
			relatives_sub_jobs.add(new_uuid);
		}
		
		return sub_job;
	}
	
	synchronized void switchToError(Exception e) {
		last_error_message = e.getMessage();
		switchStatus(TaskStatus.ERROR);
	}
	
	synchronized void switchStatus(TaskStatus new_status) {
		status = new_status;
	}
	
	public JsonObject getContextContent() {
		return context_content;
	}
	
	public ArrayList<String> getContextRequirementTags() {
		if (context_requirement_tags == null) {
			return new ArrayList<>(Collections.emptyList());
		}
		return context_requirement_tags;
	}
	
	public String getContextType() {
		return context_type;
	}
	
}
