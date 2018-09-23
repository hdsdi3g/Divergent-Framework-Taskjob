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
package tv.hd3g.divergentframework.taskjob;

import java.awt.Color;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;

import junit.framework.TestCase;
import tv.hd3g.divergentframework.taskjob.broker.Job;

public class TestLocalTaskJob extends TestCase {
	
	static class Dog {
		Color color;
		int size;
		
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (color == null ? 0 : color.hashCode());
			result = prime * result + size;
			return result;
		}
		
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Dog other = (Dog) obj;
			if (color == null) {
				if (other.color != null) {
					return false;
				}
			} else if (!color.equals(other.color)) {
				return false;
			}
			if (size != other.size) {
				return false;
			}
			return true;
		}
		
	}
	
	public void testProcess() throws InterruptedException {
		Gson gson = new Gson();
		
		Dog dogo = new Dog();
		dogo.color = Color.ORANGE;
		dogo.size = 5;
		
		ArrayList<Dog> captured_dogs = new ArrayList<>(1);
		
		InMemoryLocalTaskJob task_job = new InMemoryLocalTaskJob(10, 10, 10, 10, TimeUnit.MILLISECONDS);
		
		task_job.registerGenericEngine(1, "DogEngine", gson, Dog.class, () -> {
			return (referer, context, broker, shouldStopProcessing) -> {
				captured_dogs.add(context);
			};
		});
		
		Job job = task_job.createGenericJob("D", "", dogo, null, gson);
		
		System.out.println(job.getContextContent());
		
		assertEquals(dogo, gson.fromJson(job.getContextContent(), Dog.class));
		
		while (captured_dogs.isEmpty()) {
			Thread.onSpinWait();
		}
		
		assertEquals(dogo, captured_dogs.get(0));
	}
	
}
