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

import java.util.List;
import java.util.UUID;

import tv.hd3g.divergentframework.taskjob.broker.Job;

public interface JobEventObserver {
	
	/**
	 * Can be triggered just after onJobAfterInit, before the job has to be added to the current job list
	 */
	default void onJobUpdate(Job job, JobUpdateSubject cause) {
	}
	
	default void onJobUpdateProgression(Job job) {
	}
	
	public enum JobUpdateSubject {
		SET_EXTERNAL_REFERENCE, SET_LINKED_JOB, SET_CONTEXT_REQUIREMENT_TAGS, SWITCH_TO_ERROR, SWITCH_STATUS, SET_CONTEXT_CONTENT;
	}
	
	default void brokerOnAfterFlush(List<UUID> deleted_jobs_uuid) {
	}
	
	default void brokerOnCreateJob(Job job) {
	}
	
	default void brokerOnCreateSubJob(Job reference, Job sub_job) {
	}
	
}
