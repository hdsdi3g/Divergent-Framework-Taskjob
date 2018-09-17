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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.gson.JsonObject;

public final class Job {
	// private static Logger log = Logger.getLogger(Job.class);
	public static final String JAVA_CLASS_PREFIX_CONTEXT_TYPE = "java:class:";
	
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
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(context_type);
		sb.append(":");
		sb.append(status);
		
		if (external_reference != null) {
			sb.append(":");
			sb.append(external_reference);
		}
		sb.append(":");
		sb.append(key.toString().substring(0, 8));
		sb.append("(");
		sb.append(description.substring(0, Math.min(description.length(), 30)));
		sb.append(")");
		
		if (last_error_message != null) {
			sb.append("\"");
			sb.append(last_error_message);
			sb.append("\"");
		} else if (max_progression_value > 0) {
			sb.append(actual_progression_value);
			sb.append("/");
			sb.append(max_progression_value);
			sb.append(",");
		}
		
		if (linked_job != null) {
			sb.append("L_");
			sb.append(linked_job.toString().substring(0, 8));
		}
		if (relatives_sub_jobs != null) {
			if (relatives_sub_jobs.isEmpty() == false) {
				sb.append("R");
				sb.append(relatives_sub_jobs.size());
			}
		}
		
		return sb.toString();
	}
	
	UUID init(String description, String context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
		if (description == null) {
			this.description = "";
		} else {
			this.description = description;
		}
		
		key = UUID.randomUUID();
		
		this.context_type = context_type;
		if (context_type == null) {
			throw new NullPointerException("\"context_type\" can't to be null");
		}
		this.context_content = context_content;
		if (context_content == null) {
			this.context_content = new JsonObject();
		}
		this.context_requirement_tags = context_requirement_tags;
		
		status = TaskStatus.WAITING;
		create_date = System.currentTimeMillis();
		start_date = 0;
		end_date = 0;
		relatives_sub_jobs = new ArrayList<>();
		
		return key;
	}
	
	/**
	 * @return this
	 */
	synchronized Job setExternalReference(String external_reference) {
		this.external_reference = external_reference;
		return this;
	}
	
	/**
	 * @return this
	 */
	synchronized Job setLinkedJob(UUID linked_job) {
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
	synchronized Job updateProgression(int actual_progression_value, int max_progression_value) {
		this.actual_progression_value = actual_progression_value;
		this.max_progression_value = max_progression_value;
		return this;
	}
	
	/**
	 * @return this
	 */
	synchronized Job setContextRequirementTags(ArrayList<String> context_requirement_tags) {
		this.context_requirement_tags = context_requirement_tags;
		return this;
	}
	
	/**
	 * @return never null
	 */
	public List<Job> getRelativesJobs(Broker broker) {
		if (relatives_sub_jobs == null) {
			return Collections.emptyList();
		}
		return broker.getJobsByUUID(relatives_sub_jobs);
	}
	
	/**
	 * @return unmodifiableList, never null
	 */
	List<UUID> getRelativesJobsUUID() {
		if (relatives_sub_jobs == null) {
			return List.of();
		}
		return Collections.unmodifiableList(relatives_sub_jobs);
	}
	
	Job addSubJob(String description, String context_type, JsonObject context_content, ArrayList<String> context_requirement_tags) {
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
	
	/**
	 * @return this
	 */
	synchronized Job switchToError(Throwable e) {
		last_error_message = e.getMessage();
		switchStatus(TaskStatus.ERROR);
		return this;
	}
	
	/**
	 * @return this
	 */
	synchronized Job switchStatus(TaskStatus new_status) {
		if (status.canSwitchTo(new_status) == false) {
			throw new RuntimeException("Can't switch status from " + status + " to " + new_status);
		}
		status = new_status;
		
		if (status.statusSwitchShouldChangeJobStartDate()) {
			start_date = System.currentTimeMillis();
		}
		if (status.statusSwitchShouldChangeJobEndDate()) {
			end_date = System.currentTimeMillis();
		}
		
		return this;
	}
	
	/**
	 * @return A deep copy
	 */
	public JsonObject getContextContent() {
		return context_content.deepCopy();
	}
	
	/**
	 * @return this
	 */
	synchronized Job setContextContent(JsonObject context_content) {
		this.context_content = context_content;
		if (context_content == null) {
			throw new NullPointerException("\"context_content\" can't to be null");
		}
		return this;
	}
	
	boolean hasContextRequirementTags() {
		if (context_requirement_tags == null) {
			return false;
		}
		return context_requirement_tags.isEmpty() == false;
	}
	
	/**
	 * @return unmodifiableList, never null
	 */
	public List<String> getContextRequirementTags() {
		if (context_requirement_tags == null) {
			return List.of();
		}
		
		return Collections.unmodifiableList(context_requirement_tags);
	}
	
	public String getContextType() {
		return context_type;
	}
	
	public TaskStatus getStatus() {
		return status;
	}
	
	/**
	 * Use end_date or else create_date
	 */
	boolean isTooOld(long max_age_msec) {
		if (end_date > 0) {
			return end_date + max_age_msec < System.currentTimeMillis();
		} else {
			return create_date + max_age_msec < System.currentTimeMillis();
		}
	}
	
	public long getStartDate() {
		return start_date;
	}
	
	public long getEndDate() {
		return end_date;
	}
	
	public String getLastErrorMessage() {
		return last_error_message;
	}
	
	public String getExternalReference() {
		return external_reference;
	}
	
	public int getActualProgressionValue() {
		return actual_progression_value;
	}
	
	public int getMaxProgressionValue() {
		return max_progression_value;
	}
	
	public long getCreateDate() {
		return create_date;
	}
	
	public String getDescription() {
		return description;
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (key == null ? 0 : key.hashCode());
		return result;
	}
	
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Job)) {
			return false;
		}
		Job other = (Job) obj;
		if (key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!key.equals(other.key)) {
			return false;
		}
		return true;
	}
}
