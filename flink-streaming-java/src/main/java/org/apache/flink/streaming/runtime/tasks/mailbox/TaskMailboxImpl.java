/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.CLOSED;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.OPEN;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.QUIESCED;

/**
 * //邮箱的事项
 * Implementation of {@link TaskMailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and tailored towards
 * our use case with multiple writers and single reader.
 */
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
	/**
	 * 可重入锁
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock = new ReentrantLock();

	/**
	 * 异步执行队列
	 * Internal queue of mails.
	 */
	@GuardedBy("lock")
	private final Deque<Mail> queue = new ArrayDeque<>();

	/**
	 * 条件锁
	 * Condition that is triggered when the mailbox is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty = lock.newCondition();

	/**
	 * The state of the mailbox in the lifecycle of open, quiesced, and closed.
	 */
	@GuardedBy("lock")
	private State state = OPEN;

	/**
	 * 主线程
	 * Reference to the thread that executes the mailbox mails.
	 */
	@Nonnull
	private final Thread taskMailboxThread;

	/**
	 * 当前批队列
	 * The current batch of mails. A new batch can be created with {@link #createBatch()} and consumed with {@link
	 * #tryTakeFromBatch()}.
	 */
	private final Deque<Mail> batch = new ArrayDeque<>();

	/**
	 * Performance optimization where hasNewMail == !queue.isEmpty(). Will not reflect the state of {@link #batch}.
	 */
	private volatile boolean hasNewMail = false;

	public TaskMailboxImpl(@Nonnull final Thread taskMailboxThread) {
		this.taskMailboxThread = taskMailboxThread;
	}

	@VisibleForTesting
	public TaskMailboxImpl() {
		this(Thread.currentThread());
	}

	//是否是主线程
	@Override
	public boolean isMailboxThread() {
		return Thread.currentThread() == taskMailboxThread;
	}

	//是否有待处理
	@Override
	public boolean hasMail() {
		checkIsMailboxThread();
		return !batch.isEmpty() || hasNewMail;
	}

	//取出--先从批，再从默认队列
	//非阻塞
	@Override
	public Optional<Mail> tryTake(int priority) {
		checkIsMailboxThread();
		checkTakeStateConditions();
		Mail head = takeOrNull(batch, priority);
		if (head != null) {
			return Optional.of(head);
		}
		if (!hasNewMail) {
			return Optional.empty();
		}
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			final Mail value = takeOrNull(queue, priority);
			if (value == null) {
				return Optional.empty();
			}
			hasNewMail = !queue.isEmpty();
			return Optional.ofNullable(value);
		} finally {
			lock.unlock();
		}
	}

	//阻塞,直到有任务
	@Override
	public @Nonnull Mail take(int priority) throws InterruptedException, IllegalStateException {
		//必须在主线程完成
		checkIsMailboxThread();
		checkTakeStateConditions();
		Mail head = takeOrNull(batch, priority);
		if (head != null) {
			return head;
		}
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			Mail headMail;
			while ((headMail = takeOrNull(queue, priority)) == null) {
				//等待信息
				notEmpty.await();
			}
			hasNewMail = !queue.isEmpty();
			return headMail;
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	//是否能创建批
	@Override
	public boolean createBatch() {
		checkIsMailboxThread();
		if (!hasNewMail) {
			// batch is usually depleted by previous MailboxProcessor#runMainLoop
			// however, putFirst may add a message directly to the batch if called from mailbox thread
			return !batch.isEmpty();
		}
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			Mail mail;
			while ((mail = queue.pollFirst()) != null) {
				batch.addLast(mail);
			}
			hasNewMail = false;
			return !batch.isEmpty();
		} finally {
			lock.unlock();
		}
	}

	//从批获取
	@Override
	public Optional<Mail> tryTakeFromBatch() {
		checkIsMailboxThread();
		checkTakeStateConditions();
		return Optional.ofNullable(batch.pollFirst());
	}

	//------------------------------------------------------------------------------------------------------------------

	//增加邮件
	@Override
	public void put(@Nonnull Mail mail) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			checkPutStateConditions();
			queue.addLast(mail);
			hasNewMail = true;
			//通知
			notEmpty.signal();
		} finally {
			lock.unlock();
		}
	}

	//同步执行
	@Override
	public void putFirst(@Nonnull Mail mail) {
		if (isMailboxThread()) {
			checkPutStateConditions();
			batch.addFirst(mail);
		} else {
			final ReentrantLock lock = this.lock;
			lock.lock();
			try {
				checkPutStateConditions();
				queue.addFirst(mail);
				hasNewMail = true;
				//通知非空
				notEmpty.signal();
			} finally {
				lock.unlock();
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	//获取优先级较高的邮件
	@Nullable
	private Mail takeOrNull(Deque<Mail> queue, int priority) {
		if (queue.isEmpty()) {
			return null;
		}

		Iterator<Mail> iterator = queue.iterator();
		while (iterator.hasNext()) {
			Mail mail = iterator.next();
			if (mail.getPriority() >= priority) {
				iterator.remove();
				return mail;
			}
		}
		return null;
	}

	//所有等待队列中的任务移除
	@Override
	public List<Mail> drain() {
		List<Mail> drainedMails = new ArrayList<>(batch);
		batch.clear();
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			drainedMails.addAll(queue);
			queue.clear();
			hasNewMail = false;
			return drainedMails;
		} finally {
			lock.unlock();
		}
	}

	//检查是否是主线程
	private void checkIsMailboxThread() {
		if (!isMailboxThread()) {
			throw new IllegalStateException(
				"Illegal thread detected. This method must be called from inside the mailbox thread!");
		}
	}

	//检查状态
	private void checkPutStateConditions() {
		if (state != OPEN) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				OPEN + " for put operations.");
		}
	}

	//检查状态
	private void checkTakeStateConditions() {
		if (state == CLOSED) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				OPEN + " or " + QUIESCED + " for take operations.");
		}
	}

	//静默
	@Override
	public void quiesce() {
		checkIsMailboxThread();
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == OPEN) {
				state = QUIESCED;
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Nonnull
	@Override
	public List<Mail> close() {
		checkIsMailboxThread();
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == CLOSED) {
				return Collections.emptyList();
			}
			List<Mail> droppedMails = drain();
			state = CLOSED;
			// to unblock all
			notEmpty.signalAll();
			return droppedMails;
		} finally {
			lock.unlock();
		}
	}

	//上锁，获取状态
	@Nonnull
	@Override
	public State getState() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return state;
		} finally {
			lock.unlock();
		}
	}

	//上锁执行
	@Override
	public void runExclusively(Runnable runnable) {
		lock.lock();
		try {
			runnable.run();
		} finally {
			lock.unlock();
		}
	}
}
