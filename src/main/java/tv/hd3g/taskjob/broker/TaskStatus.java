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

public enum TaskStatus {
	
	// XXX set
	CANCELED {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case POSTPONED:
			case WAITING:
			case DONE:
				return true;
			case STOPPING:
			case STOPPED:
			case ERROR:
			case PROCESSING:
			case PREPARING:
				return false;
			default:
				return true;
			}
		}
	},
	// XXX set
	POSTPONED {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case CANCELED:
			case WAITING:
			case DONE:
				return true;
			case STOPPING:
			case STOPPED:
			case ERROR:
			case PROCESSING:
			case PREPARING:
				return false;
			default:
				return true;
			}
		}
	},
	STOPPING {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case STOPPED:
				return true;
			case CANCELED:
			case POSTPONED:
			case PREPARING:
			case WAITING:
			case DONE:
			case ERROR:
			case PROCESSING:
				return false;
			default:
				return true;
			}
		}
	},
	STOPPED {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case WAITING:
			case DONE:
				return true;
			case ERROR:
			case PREPARING:
			case PROCESSING:
			case STOPPING:
			case CANCELED:
			case POSTPONED:
				return false;
			default:
				return true;
			}
		}
	},
	ERROR {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case WAITING:
			case DONE:
				return true;
			case CANCELED:
			case POSTPONED:
			case STOPPING:
			case STOPPED:
			case PREPARING:
			case PROCESSING:
				return false;
			default:
				return true;
			}
		}
	},
	// XXX set
	PREPARING {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case PROCESSING:
			case ERROR:
				return true;
			case POSTPONED:
			case CANCELED:
			case WAITING:
			case DONE:
			case STOPPING:
			case STOPPED:
				return false;
			default:
				return true;
			}
		}
	},
	WAITING {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case CANCELED:
			case POSTPONED:
			case PREPARING:
			case PROCESSING:
				return true;
			case STOPPING:
			case STOPPED:
			case ERROR:
			case DONE:
				return false;
			default:
				return true;
			}
		}
	},
	DONE {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case WAITING:
				return true;
			case CANCELED:
			case POSTPONED:
			case STOPPING:
			case STOPPED:
			case ERROR:
			case PREPARING:
			case PROCESSING:
				return false;
			default:
				return true;
			}
		}
	},
	PROCESSING {
		public boolean canSwitchTo(TaskStatus new_status) {
			switch (new_status) {
			case ERROR:
			case STOPPING:
			case STOPPED:
			case DONE:
				return true;
			case CANCELED:
			case POSTPONED:
			case PREPARING:
			case WAITING:
				return false;
			default:
				return true;
			}
		}
	};
	
	public abstract boolean canSwitchTo(TaskStatus new_status);
	
}
