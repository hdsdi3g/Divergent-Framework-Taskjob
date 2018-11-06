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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.Message;

import tv.hd3g.divergentframework.taskjob.broker.Job;
import tv.hd3g.divergentframework.taskjob.worker.WorkerThread;

/**
 * Inspired by https://cwiki.apache.org/confluence/display/GEODE/Using+Custom+Log4J2+Appender
 */
@Plugin(name = "Basic", category = "Core", elementType = "appender", printObject = true)
public class JobEventLogAppender extends AbstractAppender {
	
	// private static Logger log = LogManager.getLogger();
	
	private final ConcurrentHashMap<Job, LinkedBlockingQueue<LogEvent>> logevents_by_job;
	private final ConcurrentHashMap<Job, Long> last_fetch_logevents_date_by_job;
	
	private Consumer<Job> onLogEvent;
	private Executor onLogEvent_executor;
	
	public JobEventLogAppender(String name, Filter filter) {
		super(name, filter, PatternLayout.createDefaultLayout());
		logevents_by_job = new ConcurrentHashMap<>();
		last_fetch_logevents_date_by_job = new ConcurrentHashMap<>();
	}
	
	public void append(LogEvent event) {
		Thread t = Thread.currentThread();
		if (t instanceof WorkerThread == false) {
			return;
		}
		WorkerThread monitored_thread = (WorkerThread) t;
		
		if (WorkerThread.class.getName().equals(event.getLoggerName())) {
			return;
		}
		
		Job job = monitored_thread.getJob();
		logevents_by_job.computeIfAbsent(job, _job -> {
			return new LinkedBlockingQueue<>();
		}).add(event.toImmutable());
		
		onLogEvent_executor.execute(() -> {
			onLogEvent.accept(job);
		});
	}
	
	public <T> List<T> getAllEvents(Job job, Function<LogEvent, T> modifier) {
		LinkedBlockingQueue<LogEvent> logevents = logevents_by_job.get(job);
		
		if (logevents == null) {
			return List.of();
		} else if (logevents.isEmpty()) {
			return List.of();
		}
		
		List<LogEvent> selected_logevents = logevents.stream().collect(Collectors.toUnmodifiableList());
		
		long last_event_date = selected_logevents.get(selected_logevents.size() - 1).getInstant().getEpochMillisecond();
		
		last_fetch_logevents_date_by_job.put(job, last_event_date);
		
		return selected_logevents.stream().map(modifier).collect(Collectors.toUnmodifiableList());
	}
	
	public <T> List<T> getAllEventsSinceLastFetch(Job job, Function<LogEvent, T> modifier) {
		LinkedBlockingQueue<LogEvent> logevents = logevents_by_job.get(job);
		
		if (logevents == null) {
			return List.of();
		} else if (logevents.isEmpty()) {
			return List.of();
		}
		
		long last_fetch_event_date = last_fetch_logevents_date_by_job.computeIfAbsent(job, _job -> {
			return 0l;
		});
		
		List<LogEvent> selected_logevents = logevents.stream().dropWhile(event -> {
			return last_fetch_event_date + 1 > event.getInstant().getEpochMillisecond();
		}).collect(Collectors.toUnmodifiableList());
		
		long last_event_date = selected_logevents.get(selected_logevents.size() - 1).getInstant().getEpochMillisecond();
		
		last_fetch_logevents_date_by_job.put(job, last_event_date);
		
		return selected_logevents.stream().map(modifier).collect(Collectors.toUnmodifiableList());
	}
	
	public synchronized JobEventLogAppender setOnLogEvent(Consumer<Job> onLogEvent, Executor onLogEvent_executor) {
		this.onLogEvent = onLogEvent;
		if (onLogEvent == null) {
			throw new NullPointerException("\"onLogEvent\" can't to be null");
		}
		this.onLogEvent_executor = onLogEvent_executor;
		if (onLogEvent_executor == null) {
			throw new NullPointerException("\"onLogEvent_executor\" can't to be null");
		}
		
		return this;
	}
	
	public void deleteDatasForJobs(List<UUID> deleted_jobs) {
		logevents_by_job.keySet().stream().filter(job -> deleted_jobs.contains(job.getKey())).collect(Collectors.toUnmodifiableList()).forEach(job -> {
			logevents_by_job.remove(job);
			last_fetch_logevents_date_by_job.remove(job);
		});
	}
	
	public static JobEventLogAppender declareAppender() {
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
		
		LocalFilter filter = new LocalFilter();
		JobEventLogAppender instance = new JobEventLogAppender("WorkerThreadJob", filter);
		instance.start();
		
		// config.addAppender(instance);
		config.getRootLogger().addAppender(instance, Level.ALL, filter);
		
		return instance;
	}
	
	private static class LocalFilter implements Filter {
		public void stop() {
		}
		
		public void start() {
		}
		
		public boolean isStopped() {
			return false;
		}
		
		public boolean isStarted() {
			return true;
		}
		
		public void initialize() {
		}
		
		public State getState() {
			return State.STARTED;
		}
		
		public Result getOnMismatch() {
			return Result.NEUTRAL;
		}
		
		public Result getOnMatch() {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1, Object p2) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0, Object p1) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String message, Object p0) {
			return Result.ACCEPT;
		}
		
		public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
			return Result.ACCEPT;
		}
		
		public Result filter(LogEvent event) {
			return Result.ACCEPT;
		}
	}
	
}
