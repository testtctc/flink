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

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/**
 * 可用性
 * Interface defining couple of essential methods for listening on data availability using
 * {@link CompletableFuture}. For usage check out for example {@link PullingAsyncDataInput}.
 */
@Internal
public interface AvailabilityProvider {
	/**
	 * Constant that allows to avoid volatile checks {@link CompletableFuture#isDone()}. Check
	 * {@link #isAvailable()} and {@link #isApproximatelyAvailable()} for more explanation.
	 */
	CompletableFuture<?> AVAILABLE = CompletableFuture.completedFuture(null);

	/**
	 * @return a future that is completed if the respective provider is available.
	 */
	CompletableFuture<?> getAvailableFuture();

	/**
	 * 是否可用
	 * In order to best-effort avoid volatile access in {@link CompletableFuture#isDone()}, we check the condition
	 * of <code>future == AVAILABLE</code> firstly for getting probable performance benefits while hot looping.
	 *
	 * <p>It is always safe to use this method in performance nonsensitive scenarios to get the precise state.
	 *
	 * @return true if this instance is available for further processing.
	 */
	default boolean isAvailable() {
		CompletableFuture<?> future = getAvailableFuture();
		return future == AVAILABLE || future.isDone();
	}

	/**
	 *
	 * Checks whether this instance is available only via constant {@link #AVAILABLE} to avoid
	 * performance concern caused by volatile access in {@link CompletableFuture#isDone()}. So it is
	 * mainly used in the performance sensitive scenarios which do not always need the precise state.
	 *
	 * <p>This method is still safe to get the precise state if {@link #getAvailableFuture()}
	 * was touched via (.get(), .wait(), .isDone(), ...) before, which also has a "happen-before"
	 * relationship with this call.
	 *
	 * @return true if this instance is available for further processing.
	 */
	default boolean isApproximatelyAvailable() {
		return getAvailableFuture() == AVAILABLE;
	}

	/**
	 * 实现
	 * A availability implementation for providing the helpful functions of resetting the
	 * available/unavailable states.
	 */
	final class AvailabilityHelper implements AvailabilityProvider {
		//内建
		private CompletableFuture<?> availableFuture = new CompletableFuture<>();

		/**
		 * 重设不可用
		 * Judges to reset the current available state as unavailable.
		 */
		public void resetUnavailable() {
			if (isAvailable()) {
				availableFuture = new CompletableFuture<>();
			}
		}

		/**
		 * 重设可用性
		 * Resets the constant completed {@link #AVAILABLE} as the current state.
		 */
		public void resetAvailable() {
			availableFuture = AVAILABLE;
		}

		/**
		 * 设定完成
		 *  Returns the previously not completed future and resets the constant completed
		 *  {@link #AVAILABLE} as the current state.
		 */
		public CompletableFuture<?> getUnavailableToResetAvailable() {
			CompletableFuture<?> toNotify = availableFuture;
			availableFuture = AVAILABLE;
			return toNotify;
		}

		/**
		 * 重设未完成
		 *  Creates a new uncompleted future as the current state and returns the
		 *  previous uncompleted one.
		 */
		public CompletableFuture<?> getUnavailableToResetUnavailable() {
			CompletableFuture<?> toNotify = availableFuture;
			availableFuture = new CompletableFuture<>();
			return toNotify;
		}

		/**
		 * 返回
		 * @return a future that is completed if the respective provider is available.
		 */
		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return availableFuture;
		}
	}
}
