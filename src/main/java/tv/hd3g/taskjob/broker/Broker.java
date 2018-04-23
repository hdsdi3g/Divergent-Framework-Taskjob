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
package tv.hd3g.taskjob.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import tv.hd3g.taskjob.Job;
import tv.hd3g.taskjob.TaskStatus;

public interface Broker {// TODO implements broker
	
	List<Job> getJobsByUUID(Job job, List<UUID> keys);
	
	public Job createJob(String description, String external_reference, Object context, ArrayList<String> context_requirement_tags);
	
	public Job addAction(Job reference, String description, Object context, ArrayList<String> context_requirement_tags);
	
	public List<Job> getAllJobs();
	
	public void updateProgression(Job action, int actual_value, int max_value);
	
	public void switchToError(Job job, Throwable e);
	
	public void switchStatus(Job job, TaskStatus new_status);
	
	/**
	 * Must be atomic.
	 * Use current pending Actions list, and starts, if needed, waiting jobs for create new pending Actions.
	 * @param list_to_context_types: types that queue can handle actually
	 * @param filterByContextTypeAndTags: if can select context_type -> context requirements tags
	 * @param onFoundActionReadyToStart: selected and locked actions are presented to queue, and return true if action is really started.
	 */
	public void getNextActions(List<String> list_to_context_types, BiPredicate<String, List<String>> filterByContextTypeAndTags, Predicate<Job> onFoundActionReadyToStart);
	
	public void onNewLocalJobsActivity(Runnable callback);
	
}
