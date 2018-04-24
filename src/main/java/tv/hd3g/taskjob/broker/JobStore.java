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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.log4j.Logger;

class JobStore {
	private static Logger log = Logger.getLogger(JobStore.class);
	
	private final ReentrantLock lock;
	private final Condition lock_condition;
	private boolean in_write_operation;
	
	private final HashMap<UUID, JobEntry> jobs_by_uuid;
	private final JobList waiting_jobs;
	private final JobList done_jobs;
	private final JobList others_jobs;
	
	JobStore() {
		lock = new ReentrantLock();
		lock_condition = lock.newCondition();
		
		jobs_by_uuid = new HashMap<>();
		waiting_jobs = new JobList();
		done_jobs = new JobList();
		others_jobs = new JobList();
	}
	
	private class JobEntry {
		final Job job;
		JobEntry previous;
		JobEntry next;
		
		JobEntry(Job job, JobEntry previous) {
			this.job = job;
			this.previous = previous;
			next = null;
		}
	}
	
	private class JobList {
		JobEntry first;
		
		final HashSet<UUID> items;
		
		JobList() {
			first = null;
			items = new HashSet<>();
		}
		
		/**
		 * Not Thread safe
		 */
		class Iterate implements Iterator<JobEntry> {
			
			JobEntry current;
			
			public boolean hasNext() {
				if (current == null) {
					current = first;
				} else {
					current = current.next;
				}
				
				return current != null;
			}
			
			public JobEntry next() {
				return current;
			}
			
		}
		
		/**
		 * Not Thread safe
		 */
		Stream<Job> stream() {
			Stream<JobEntry> entries = StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterate(), Spliterator.IMMUTABLE + Spliterator.DISTINCT + Spliterator.NONNULL), false);
			
			return entries.map(job_entry -> {
				return job_entry.job;
			});
		}
	}
	
	private <T> T syncRead(Supplier<T> compute) {
		lock.lock();
		
		try {
			while (in_write_operation) {
				lock_condition.await();
			}
			
			return compute.get();
		} catch (InterruptedException ie) {
			throw new RuntimeException("Cancel get", ie);
		} finally {
			lock.unlock();
		}
	}
	
	private JobList getListByTaskStatus(TaskStatus status) {
		if (TaskStatus.DONE.equals(status)) {
			return done_jobs;
		} else if (TaskStatus.WAITING.equals(status)) {
			return waiting_jobs;
		} else {
			return others_jobs;
		}
	}
	
	private <T> T syncWrite(Supplier<T> compute) {
		lock.lock();
		
		try {
			while (in_write_operation) {
				lock_condition.await();
			}
			in_write_operation = true;
			
			return compute.get();
		} catch (InterruptedException ie) {
			throw new RuntimeException("Cancel write", ie);
		} finally {
			in_write_operation = false;
			try {
				lock_condition.signalAll();
			} finally {
				lock.unlock();
			}
		}
	}
	
	boolean put(Job job) {
		return syncWrite(() -> {
			if (jobs_by_uuid.containsKey(job.getKey())) {
				return false;
			}
			
			JobEntry job_entry = new JobEntry(job, null);
			jobs_by_uuid.put(job.getKey(), job_entry);
			
			JobList current_list = getListByTaskStatus(job.getStatus());
			job_entry.next = current_list.first;
			current_list.first = job_entry;
			current_list.items.add(job.getKey());
			
			return true;
		});
	}
	
	/**
	 * Not thread safe.
	 */
	private void removeFromList(JobList list, JobEntry job_entry) {
		UUID uuid = job_entry.job.getKey();
		if (list.items.contains(uuid) == false) {
			return;
		}
		list.items.remove(uuid);
		
		JobEntry next = job_entry.next;
		if (list.first == job_entry) {
			list.first = next;
			if (next != null) {
				next.previous = null;
			}
		} else {
			JobEntry previous = job_entry.previous;
			if (next != null) {
				next.previous = previous;
			}
			previous.next = next;
		}
		
		job_entry.previous = null;
		job_entry.next = null;
	}
	
	/**
	 * @param target supplier call is in internal lock
	 */
	boolean update(Supplier<UUID> target) {
		return syncWrite(() -> {
			UUID uuid = target.get();
			if (jobs_by_uuid.containsKey(uuid) == false) {
				return false;
			}
			
			JobEntry job_entry = jobs_by_uuid.get(uuid);
			JobList planned_list = getListByTaskStatus(job_entry.job.getStatus());
			
			if (planned_list.items.contains(uuid) == false) {
				if (planned_list.equals(done_jobs) == false) {
					removeFromList(done_jobs, job_entry);
				}
				if (planned_list.equals(waiting_jobs) == false) {
					removeFromList(waiting_jobs, job_entry);
				}
				if (planned_list.equals(others_jobs) == false) {
					removeFromList(others_jobs, job_entry);
				}
				
				job_entry.next = planned_list.first;
				planned_list.first = job_entry;
				planned_list.items.add(uuid);
			}
			
			return true;
		});
	}
	
	Job getByUUID(UUID uuid) {
		return syncRead(() -> {
			if (jobs_by_uuid.containsKey(uuid)) {
				return jobs_by_uuid.get(uuid).job;
			}
			return null;
		});
	}
	
	int size() {
		return syncRead(() -> {
			return jobs_by_uuid.size();
		});
	}
	
	List<Job> getJobsByTaskStatus(TaskStatus status) {
		return syncRead(() -> {
			return getListByTaskStatus(status).stream().filter(job -> {
				return job.getStatus().equals(status);
			}).collect(Collectors.toList());
		});
	}
	
	/**
	 * @param stream_processor dunction call is in internal lock
	 */
	<T> T getFromJobs(TaskStatus status, Function<Stream<Job>, T> stream_processor) {
		return syncRead(() -> {
			return stream_processor.apply(getListByTaskStatus(status).stream().filter(job -> {
				return job.getStatus().equals(status);
			}));
		});
	}
	
}
