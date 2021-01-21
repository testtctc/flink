/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ScheduledFuture;

/**
 * Defines the current processing time and handles all related actions,
 * such as register timers for tasks to be executed in the future.
 *
 * <p>The access to the time via {@link #getCurrentProcessingTime()} is always available, regardless of
 * whether the timer service has been shut down.
 */
public interface ProcessingTimeService {

	/**
	 * 获取当前时间点
	 * Returns the current processing time.
	 */
	long getCurrentProcessingTime();

	/**
	 * 注册任务
	 * 任务可以取消
	 * Registers a task to be executed when (processing) time is {@code timestamp}.
	 *
	 * @param timestamp   Time when the task is to be executed (in processing time)
	 * @param target      The task to be executed
	 *
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target);

	/**
	 * 固定的间隔注册任务
	 * Registers a task to be executed repeatedly at a fixed rate.
	 *
	 * @param callback to be executed after the initial delay and then after each period
	 * @param initialDelay initial delay to start executing callback
	 * @param period after the initial delay after which the callback is executed
	 * @return Scheduled future representing the task to be executed repeatedly
	 */
	ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period);
}
