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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

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

/**
 * Inspired by https://cwiki.apache.org/confluence/display/GEODE/Using+Custom+Log4J2+Appender
 */
@Plugin(name = "Basic", category = "Core", elementType = "appender", printObject = true)
public class JobEventLogAppender extends AbstractAppender {
	
	// private static Logger log = LogManager.getLogger();
	
	private final ConcurrentHashMap<Job, LinkedBlockingQueue<LogEvent>> logevents_by_job;
	private final ConcurrentHashMap<Thread, Boolean> monitored_threads;
	
	public JobEventLogAppender(String name, Filter filter) {
		super(name, filter, PatternLayout.createDefaultLayout());
		logevents_by_job = new ConcurrentHashMap<>();
		monitored_threads = new ConcurrentHashMap<>();
	}
	
	public void monitorThreadToLog(Thread t) {
		monitored_threads.put(t, true);
	}
	
	public void unMonitorThreadToLog(Thread t) {
		monitored_threads.remove(t);
	}
	
	public void append(LogEvent event) {
		if (monitored_threads.containsKey(Thread.currentThread()) == false) {
			return;
		}
		// XXX create specific and contextual logger, do something custom
		System.out.println("+++\t" + Thread.currentThread().getName() + "\t\t" + event.getThreadName() + "\t" + event.getMessage().getFormattedMessage());
	}
	
	public static JobEventLogAppender declareAppender() {
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
		
		LocalFilter filter = new LocalFilter();
		JobEventLogAppender instance = new JobEventLogAppender("WorkerThreadJob", filter);
		instance.start();
		
		config.addAppender(instance);
		config.getLoggers().values().forEach(logger_config -> {
			logger_config.addAppender(instance, Level.ALL, filter);
		});
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
