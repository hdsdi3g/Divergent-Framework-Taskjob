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

import java.io.Serializable;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.Message;

/**
 * Inspired by https://cwiki.apache.org/confluence/display/GEODE/Using+Custom+Log4J2+Appender
 */
@Plugin(name = "Basic", category = "Core", elementType = "appender", printObject = true)
public class JobEventLogAppender extends AbstractAppender {
	
	// private static Logger log = LogManager.getLogger();
	
	private static volatile JobEventLogAppender instance;
	
	public JobEventLogAppender(final String name, final Filter filter, final Layout<? extends Serializable> layout) {
		super(name, filter, layout);
	}
	
	@PluginFactory
	public static JobEventLogAppender createAppender(@PluginAttribute("name") String name, @PluginAttribute("ignoreExceptions") boolean ignoreExceptions, @PluginElement("Layout") Layout<?> layout, @PluginElement("Filters") Filter filter) {
		if (layout == null) {
			layout = PatternLayout.createDefaultLayout();
		}
		
		instance = new JobEventLogAppender(name, filter, layout);
		return instance;
	}
	
	public static JobEventLogAppender getInstance() {
		return instance;
	}
	
	public void append(final LogEvent event) {
		// do something custom
		System.out.println("+++\t" + event.getThreadName() + "\t" + event.getMessage().getFormattedMessage());
	}
	
	public static void declareAppender() {
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
		
		config.addAppender(createAppender("WorkerThreadJob", true, null, new Filter() {
			
			public void stop() {
				// TODO Auto-generated method stub
				
			}
			
			public void start() {
				// TODO Auto-generated method stub
				
			}
			
			public boolean isStopped() {
				return false;
			}
			
			public boolean isStarted() {
				return true;
			}
			
			public void initialize() {
				// TODO Auto-generated method stub
				
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
		}));
		
		// XXX create specific and contextual logger
		/*config.addAppender(appender);
		AppenderRef ref = AppenderRef.createAppenderRef("File", null, null);
		AppenderRef[] refs = new AppenderRef[] { ref };
		LoggerConfig loggerConfig = LoggerConfig.createLogger("false", "info", "org.apache.logging.log4j", "true", refs, null, config, null);
		loggerConfig.addAppender(appender, null, null);
		config.addLogger("org.apache.logging.log4j", loggerConfig);
		ctx.updateLoggers();*/
	}
	
}
