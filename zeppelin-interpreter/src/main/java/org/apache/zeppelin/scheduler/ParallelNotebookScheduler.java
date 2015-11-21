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

package org.apache.zeppelin.scheduler;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import org.apache.zeppelin.scheduler.Job.Status;

/**
 *
 */

public class ParallelNotebookScheduler implements Scheduler {
  List<Job> queue = new LinkedList<Job>();
  List<Job> running = new LinkedList<Job>();
  Set<String> runningNoteBook = new HashSet<String>();
  private ExecutorService executor;
  private SchedulerListener listener;
  boolean terminate = false;
  private String name;
  private int maxConcurrency;

  public ParallelNotebookScheduler(String name, ExecutorService executor,
                                   SchedulerListener listener, int maxConcurrency) {
    this.name = name;
    this.executor = executor;
    this.listener = listener;
    this.maxConcurrency = maxConcurrency;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<Job> getJobsWaiting() {
    List<Job> ret = new LinkedList<Job>();
    synchronized (queue) {
      for (Job job : queue) {
        ret.add(job);
      }
    }
    return ret;
  }

  @Override
  public Collection<Job> getJobsRunning() {
    List<Job> ret = new LinkedList<Job>();
    synchronized (queue) {
      for (Job job : running) {
        ret.add(job);
      }
    }
    return ret;
  }



  @Override
  public void submit(Job job) {
    job.setStatus(Status.PENDING);
    synchronized (queue) {
      queue.add(job);
      queue.notify();
    }
  }

  private Job getJobFromQueue(List<Job> queue) {
    if ((queue.get(0).getParentName() == null)) {
      return queue.get(0);
    }
    for (Job job : queue) {
      if (job.getStatus() == Status.PENDING) {
        job.setStatus(Status.QUEUED);
      }
      if (runningNoteBook.contains(job.getParentName())) {
        continue;
      }
      return job;
    }
    return null;
  }

  @Override
  public void run() {

    synchronized (queue) {
      while (terminate == false) {
        if (running.size() >= maxConcurrency || queue.isEmpty() == true) {
          try {
            queue.wait(500);
          } catch (InterruptedException e) {
          }
          continue;
        }

        Job job = getJobFromQueue(queue);
        if (job != null) {
          queue.remove(job);
          runningNoteBook.add(job.getParentName());
          running.add(job);
          Scheduler scheduler = this;
          executor.execute(new JobRunner(scheduler, job));
        }
        else {
          try {
            queue.wait(500);
          } catch (InterruptedException e) {
          }
        }
      }


    }
  }

  public void setMaxConcurrency(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
    synchronized (queue) {
      queue.notify();
    }
  }

  private class JobRunner implements Runnable {
    private Scheduler scheduler;
    private Job job;

    public JobRunner(Scheduler scheduler, Job job) {
      this.scheduler = scheduler;
      this.job = job;
    }

    @Override
    public void run() {
      if (job.isAborted()) {
        job.setStatus(Status.ABORT);
        job.aborted = false;

        synchronized (queue) {
          running.remove(job);
          runningNoteBook.remove(job.getParentName());
          queue.notify();
        }

        return;
      }

      job.setStatus(Status.RUNNING);
      if (listener != null) {
        listener.jobStarted(scheduler, job);
      }
      job.run();
      if (job.isAborted()) {
        job.setStatus(Status.ABORT);
      } else {
        if (job.getException() != null) {
          job.setStatus(Status.ERROR);
        } else {
          job.setStatus(Status.FINISHED);
        }
      }

      if (listener != null) {
        listener.jobFinished(scheduler, job);
      }

      // reset aborted flag to allow retry
      job.aborted = false;
      synchronized (queue) {
        running.remove(job);
        runningNoteBook.remove(job.getParentName());
        queue.notify();
      }
    }
  }


  @Override
  public void stop() {
    terminate = true;
    synchronized (queue) {
      queue.notify();
    }
  }

}
