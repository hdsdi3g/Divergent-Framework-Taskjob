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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * In memory.
 */
class JobStore {
	private static final Logger log = LogManager.getLogger();
	
	private final ReentrantLock lock;
	private final Condition lock_condition;
	private boolean in_write_operation;
	
	private final HashMap<UUID, Job> jobs_by_uuid;
	private final HashSet<UUID> waiting_jobs;
	private final HashSet<UUID> others_jobs;
	
	JobStore() {
		lock = new ReentrantLock();
		lock_condition = lock.newCondition();
		
		jobs_by_uuid = new HashMap<>();
		waiting_jobs = new HashSet<>();
		others_jobs = new HashSet<>();
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
	
	private HashSet<UUID> getListByTaskStatus(TaskStatus status) {
		if (TaskStatus.WAITING.equals(status)) {
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
			
			jobs_by_uuid.put(job.getKey(), job);
			getListByTaskStatus(job.getStatus()).add(job.getKey());
			
			return true;
		});
	}
	
	/**
	 * @return Not thread safe.
	 */
	private boolean update(UUID uuid) {
		if (jobs_by_uuid.containsKey(uuid) == false) {
			return false;
		}
		
		Job job = jobs_by_uuid.get(uuid);
		HashSet<UUID> planned_list = getListByTaskStatus(job.getStatus());
		
		if (planned_list.contains(uuid) == false) {
			if (planned_list.equals(waiting_jobs) == false) {
				waiting_jobs.remove(job.getKey());
			}
			if (planned_list.equals(others_jobs) == false) {
				others_jobs.remove(job.getKey());
			}
			planned_list.add(uuid);
		}
		
		return true;
	}
	
	/**
	 * @param target supplier call is in internal lock
	 */
	boolean update(Supplier<UUID> target) {
		return syncWrite(() -> {
			return update(target.get());
		});
	}
	
	Job getByUUID(UUID uuid) {
		return syncRead(() -> {
			return jobs_by_uuid.get(uuid);
		});
	}
	
	int size() {
		return syncRead(() -> {
			return jobs_by_uuid.size();
		});
	}
	
	int waitingJobCount() {
		return syncRead(() -> {
			return waiting_jobs.size();
		});
	}
	
	List<Job> getJobsByTaskStatus(TaskStatus status) {
		return syncRead(() -> {
			return getListByTaskStatus(status).stream().map(uuid -> {
				return jobs_by_uuid.get(uuid);
			}).filter(job -> {
				return job.getStatus().equals(status);
			}).collect(Collectors.toList());
		});
	}
	
	/**
	 * @param stream_processor dunction call is in internal lock
	 */
	<T> T getFromJobs(TaskStatus status, Function<Stream<Job>, T> stream_processor) {
		return syncRead(() -> {
			return stream_processor.apply(getListByTaskStatus(status).stream().map(uuid -> {
				return jobs_by_uuid.get(uuid);
			}).filter(job -> {
				return job.getStatus().equals(status);
			}));
		});
	}
	
	/**
	 * Please dont output a stream: it will not be synchonised.
	 * @param stream_processor function call is in internal lock.
	 */
	<T> T getFromJobs(Function<Stream<Job>, T> stream_processor) {
		return syncRead(() -> {
			return stream_processor.apply(jobs_by_uuid.keySet().stream().map(uuid -> {
				return jobs_by_uuid.get(uuid);
			}));
		});
	}
	
	void checkConsistency() {
		if (jobs_by_uuid.size() != waiting_jobs.size() + others_jobs.size()) {
			throw new RuntimeException("Invalid lists sizes, jobs_by_uuid: " + jobs_by_uuid.size() + ", waiting_jobs: " + waiting_jobs.size() + ", others_jobs: " + others_jobs.size());
		}
		
		waiting_jobs.forEach(uuid -> {
			if (jobs_by_uuid.containsKey(uuid) == false) {
				throw new NullPointerException("Missing " + uuid + " from waiting_jobs in jobs_by_uuid");
			} else if (jobs_by_uuid.get(uuid).getStatus() != TaskStatus.WAITING) {
				throw new RuntimeException("Invalid job list position, " + jobs_by_uuid.get(uuid) + " can't be in WAITING list because it's in " + jobs_by_uuid.get(uuid).getStatus() + " state");
			}
		});
		
		others_jobs.forEach(uuid -> {
			if (jobs_by_uuid.containsKey(uuid) == false) {
				throw new NullPointerException("Missing " + uuid + " from others_jobs in jobs_by_uuid");
			} else if (jobs_by_uuid.get(uuid).getStatus() == TaskStatus.WAITING | jobs_by_uuid.get(uuid).getStatus() == TaskStatus.DONE) {
				throw new RuntimeException("Invalid job list position, " + jobs_by_uuid.get(uuid) + " can't be in OTHER list because it's in " + jobs_by_uuid.get(uuid).getStatus() + " state");
			}
		});
	}
	
	void computeAndRemove(TaskStatus status, Function<Stream<Job>, Stream<Job>> stream_processor) {
		syncWrite(() -> {
			HashSet<UUID> task_list = getListByTaskStatus(status);
			
			stream_processor.apply(task_list.stream().map(uuid -> {
				return jobs_by_uuid.get(uuid);
			})).collect(Collectors.toList()).stream().forEach(job -> {
				if (log.isTraceEnabled()) {
					log.trace("Remove job " + job);
				}
				jobs_by_uuid.remove(job.getKey());
				task_list.remove(job.getKey());
			});
			
			return null;
		});
	}
	
	/**
	 * @param stream_processor (full_job_list_stream_to_filter, job_by_uuid_resolver) -> filtered stream to remove
	 */
	void computeAllAndRemove(BiFunction<Stream<Job>, Function<UUID, Job>, Stream<Job>> stream_processor) {
		syncWrite(() -> {
			Set<UUID> task_list = jobs_by_uuid.keySet();
			
			stream_processor.apply(task_list.stream().map(uuid -> {
				return jobs_by_uuid.get(uuid);
			}), uuid -> {
				return jobs_by_uuid.get(uuid);
			}).collect(Collectors.toList()).stream().forEach(job -> {
				if (log.isTraceEnabled()) {
					log.trace("Remove job " + job);
				}
				jobs_by_uuid.remove(job.getKey());
				task_list.remove(job.getKey());
			});
			
			return null;
		});
	}
	
	/**
	 * @param stream_processor (status' filtered job_list_stream_to_filter, job_by_uuid_resolver) -> filtered stream to update
	 * @return selected jobs
	 */
	List<Job> computeAndUpdate(TaskStatus status, BiFunction<Stream<Job>, Function<UUID, Job>, Stream<Job>> stream_processor, Consumer<Job> toUpdate) {
		return syncWrite(() -> {
			HashSet<UUID> task_list = getListByTaskStatus(status);
			
			/**
			 * Search jobs
			 */
			List<Job> sub_list = stream_processor.apply(task_list.stream().map(uuid -> {
				return jobs_by_uuid.get(uuid);
			}), uuid -> {
				return jobs_by_uuid.get(uuid);
			}).collect(Collectors.toList());
			
			// ConcurrentModificationException if stream.toList is plugged
			
			/**
			 * Update and save jobs
			 */
			sub_list.stream().peek(toUpdate).forEach(job -> {
				if (log.isTraceEnabled()) {
					log.trace("Update job " + job);
				}
				
				if (update(job.getKey()) == false) {
					throw new RuntimeException("Can't update job " + job);
				}
			});
			
			return sub_list;
		});
	}
	
}
