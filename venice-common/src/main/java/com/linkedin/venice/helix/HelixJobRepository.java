package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataChangedListener;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.VeniceSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Job Repository which persist job data on Helix. This repository also listen the external view so that it can update
 * the partition and instances information for job when nodes are failed or assigned to the related resource.
 */
public class HelixJobRepository implements JobRepository, RoutingDataChangedListener {
  private static final Logger logger = Logger.getLogger(HelixJobRepository.class);

  private Map<String, List<Job>> topicToRunningJobsMap;

  private Map<String, List<Job>> topicToTerminatedJobsMap;

  private final ZkBaseDataAccessor<OfflineJob> jobDataAccessor;

  private final ZkBaseDataAccessor<List<Task>> tasksDataAccessor;

  private final String offlineJobsPath;

  private final HelixAdapterSerializer adapter;

  private final RoutingDataRepository routingDataRepository;

  public static final String OFFLINE_JOBS_SUB_PATH = "/OfflineJobs";

  //TODO Add the serializer for near-line job later.
  public HelixJobRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, RoutingDataRepository routingDataRepository) {
    this(zkClient, adapter, clusterName, routingDataRepository, new OfflineJobJSONSerializer(),
        new TasksJSONSerializer());
  }

  public HelixJobRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, RoutingDataRepository routingDataRepository,
      VeniceSerializer<OfflineJob> jobSerializer, VeniceSerializer<List<Task>> taskVeniceSerializer) {
    this.routingDataRepository = routingDataRepository;
    offlineJobsPath = "/" + clusterName + OFFLINE_JOBS_SUB_PATH;
    this.adapter = adapter;
    this.adapter.registerSerializer(offlineJobsPath, jobSerializer);
    this.adapter.registerSerializer(offlineJobsPath + "/", taskVeniceSerializer);
    zkClient.setZkSerializer(this.adapter);
    jobDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    tasksDataAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  @Override
  public synchronized List<Job> getRunningJobOfTopic(@NotNull String kafkaTopic) {
    List<Job> jobs = topicToRunningJobsMap.get(kafkaTopic);

    if (jobs == null) {
      //No running jobs in this topic.
      throw new VeniceException("Can not find running job for kafka topic:" + kafkaTopic);
    }
    return jobs;
  }

  @Override
  public List<Job> getTerminatedJobOfTopic(String kafkaTopic) {
    List<Job> jobs = topicToTerminatedJobsMap.get(kafkaTopic);
    if (jobs == null) {
      //No running jobs in this topic.
      throw new VeniceException("Can not find running job for kafka topic:" + kafkaTopic);
    }
    return jobs;
  }

  @Override
  public synchronized void archiveJob(long jobId, String kafkaTopic) {
    Job job = this.getJob(jobId, kafkaTopic);
    job.verifyNewJobStatus(ExecutionStatus.ARCHIVED);
    ExecutionStatus oldStatus = job.getStatus();
    job.setStatus(ExecutionStatus.ARCHIVED);
    try {
      updateJobToZK(job);
    } catch (Throwable e) {
      String errorMessage = "Can not update job:" + job.getJobId() + " to ZK.";
      logger.info(errorMessage, e);
      job.setStatus(oldStatus);
      throw new VeniceException(errorMessage, e);
    }
    deleteJobFromMap(kafkaTopic, jobId, topicToTerminatedJobsMap);
    try {
      removeJobFromZK(job);
    } catch (Throwable e) {
      String errorMessage = "Can not remove job:" + job.getJobId() + " from ZK.";
      logger.info(errorMessage, e);
      List<Job> jobs = getAndCreateJobListFromMap(kafkaTopic, topicToTerminatedJobsMap);
      jobs.add(job);
      throw new VeniceException(errorMessage, e);
    }
  }

  @Override
  public synchronized void updateTaskStatus(long jobId, String kafkaTopic, @NotNull Task task) {
    Job job = this.getJob(jobId, kafkaTopic);
    Task oldTask = job.getTask(task.getPartitionId(), task.getInstanceId());
    job.updateTaskStatus(task);
    //Write updates to ZK at first.
    try {
      updateTaskToZK(job.getJobId(), task.getPartitionId(), job.tasksInPartition(task.getPartitionId()));
    } catch (Throwable e) {
      String errorMessage = "Can not update task:" + task.getTaskId() + ". Rollback local copy.";
      logger.info(errorMessage, e);
      //If met any error when updating task to ZK, rollback local copy.
      if (oldTask == null) {
        job.deleteTask(task);
      } else {
        job.addTask(oldTask);
      }
      throw new VeniceException(errorMessage, e);
    }
  }

  @Override
  public synchronized void stopJob(long jobId, String kafkaTopic) {
    internalStopJob(jobId, kafkaTopic, ExecutionStatus.COMPLETED);
  }

  @Override
  public synchronized void stopJobWithError(long jobId, String kafkaTopic) {
    internalStopJob(jobId, kafkaTopic, ExecutionStatus.ERROR);
  }

  private void internalStopJob(long jobId, String kafkaTopic, ExecutionStatus status) {
    Job job = this.getJob(jobId, kafkaTopic);
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    job.verifyNewJobStatus(status);
    ExecutionStatus oldStaus = job.getStatus();
    job.setStatus(status);
    try {
      updateJobToZK(job);
    } catch (Throwable e) {
      String errorMessage = "Can not update job:" + job.getJobId() + " to ZK. Rollback the local copy.";
      logger.info(errorMessage, e);
      //Ignore verification here. Becasue we need to rollback to original status.
      job.setStatus(oldStaus);
      throw new VeniceException(errorMessage, e);
    }
    //Remove from running jobs and add it to terminated jobs.
    deleteJobFromMap(kafkaTopic, job.getJobId(), topicToRunningJobsMap);
    List<Job> jobs = getAndCreateJobListFromMap(kafkaTopic, topicToTerminatedJobsMap);
    jobs.add(job);
  }

  @Override
  public void startJob(@NotNull Job job) {
    if (!job.getStatus().equals(ExecutionStatus.NEW)) {
      throw new VeniceException("Job:" + job.getJobId() + " is in " + job.getStatus() + ". Can not be started.");
    }
    synchronized (this) {
      List<Job> jobs = getAndCreateJobListFromMap(job.getKafkaTopic(), topicToRunningJobsMap);
      jobs.add(job);
    }

    waitUntilJobStart(job);
    //After job being started, sync up it to ZK.
    try {
      job.verifyNewJobStatus(ExecutionStatus.STARTED);
      job.setStatus(ExecutionStatus.STARTED);
      updateJobToZK(job);
      for (int partitionId = 0; partitionId < job.getNumberOfPartition(); partitionId++) {
        updateTaskToZK(job.getJobId(), partitionId, new ArrayList<>());
      }
    } catch (Throwable e) {
      String errorMessage = "Can not update Job:" + job.getJobId() + " to ZK.";
      logger.info(errorMessage, e);
      //Roll back local copy
      deleteJobFromMap(job.getKafkaTopic(), job.getJobId(), topicToRunningJobsMap);
      throw new VeniceException(errorMessage, e);
    }
  }

  /**
   * When we creating a new job for new helix resource. We need to know how many tasks we need to create and which
   * instance is assigned to execute this task. Unfortunately controller need some time to wait all of participants
   * assigned to this resource become ONLINE, then get these information from external view, otherwise the external view
   * is empty or uncompleted.
   *
   * @param job
   */
  private void waitUntilJobStart(Job job) {

    //There are no enough partitions and/or replicas to execute tasks. Wait until all of replicas becoming online
    synchronized (job) {
      routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
      boolean isJobStarted = false;
      if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic())) {
        try {
          job.updateExecutingPartitions(routingDataRepository.getPartitions(job.getKafkaTopic()));
          isJobStarted = true;
        } catch (VeniceException e) {
          // Can not get enough partition and replicas when resource just being created.
        }
      }
      if (!isJobStarted) {
        try {
          logger.info("Wait job:" + job.getJobId() + " being started.");
          job.wait();
          logger.info("Job:" + job.getJobId() + " could be started.");
        } catch (InterruptedException e) {
          throw new VeniceException("Met error when wait job being started.", e);
        }
      }
    }
  }

  @Override
  public synchronized ExecutionStatus getJobStatus(long jobId, String kafkaTopic) {
    Job job = this.getJob(jobId, kafkaTopic);
    return job.getStatus();
  }

  @Override
  public synchronized Job getJob(long jobId, String kafkaTopic) {
    Job job = getJobFromMap(kafkaTopic, jobId, topicToRunningJobsMap);
    if (job != null) {
      return job;
    }
    job = getJobFromMap(kafkaTopic, jobId, topicToTerminatedJobsMap);
    if (job != null) {
      return job;
    }
    throw new VeniceException("Job:" + jobId + " dose not exist.");
  }

  private void updateJobToZK(Job job) {
    if (job instanceof OfflineJob) {
      jobDataAccessor.set(offlineJobsPath + "/" + job.getJobId(), (OfflineJob) job, AccessOption.PERSISTENT);
    } else {
      throw new VeniceException("Only offline job is supported now.");
    }
  }

  public void removeJobFromZK(Job job) {
    if (job instanceof OfflineJob) {
      jobDataAccessor.remove(offlineJobsPath + "/" + job.getJobId(), AccessOption.PERSISTENT);
    } else {
      throw new VeniceException("Only offline job is supported now.");
    }
  }

  private void updateTaskToZK(long jobId, int partitionId, List<Task> tasks) {
    tasksDataAccessor.set(offlineJobsPath + "/" + jobId + "/" + partitionId, tasks, AccessOption.PERSISTENT);
  }

  public synchronized void start() {
    topicToRunningJobsMap = new HashMap<>();
    topicToTerminatedJobsMap = new HashMap<>();
    logger.info("Start getting offline jobs from ZK");
    // We don't need to listen the change of jobs and tasks. The master controller is the only entrance to read/write
    // these data. When master is failed, another controller will take over this mastership and load from ZK when
    // becoming master.
    List<OfflineJob> offLineJobs = jobDataAccessor.getChildren(offlineJobsPath, null, AccessOption.PERSISTENT);
    logger.info("Get " + offLineJobs.size() + " offline jobs.");
    for (OfflineJob job : offLineJobs) {
      if (job.getStatus().equals(ExecutionStatus.ARCHIVED)) {
        //Archived job, do not add it to repository.
        continue;
      }

      List<Job> jobs;
      if (job.isTerminated()) {
        jobs = getAndCreateJobListFromMap(job.getKafkaTopic(), topicToTerminatedJobsMap);
      } else {
        jobs = getAndCreateJobListFromMap(job.getKafkaTopic(), topicToRunningJobsMap);
      }
      jobs.add(job);

      logger.info("Start getting tasks for job:" + job.getJobId());
      List<List<Task>> tasks =
          tasksDataAccessor.getChildren(offlineJobsPath + "/" + job.getJobId(), null, AccessOption.PERSISTENT);
      for (List<Task> task : tasks) {
        task.forEach(job::addTask);
      }
      if (job.getStatus().equals(ExecutionStatus.NEW)) {
        //Wait and start job.
        waitUntilJobStart(job);
      } else {
        //Get the newest partitions info from repository and update the job if needed.
        updateJobPartitions(job, routingDataRepository.getPartitions(job.getKafkaTopic()));
        routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
      }
      logger.info("Filled tasks into job:" + job.getJobId());
      ExecutionStatus jobStatus = job.checkJobStatus();
      if (jobStatus.equals(ExecutionStatus.COMPLETED)) {
        stopJob(job.getJobId(), job.getKafkaTopic());
      } else if (jobStatus.equals(ExecutionStatus.ERROR)) {
        stopJobWithError(job.getJobId(), job.getKafkaTopic());
      }
    }
    logger.info("End getting offline jobs from zk");
  }

  private List<Job> getAndCreateJobListFromMap(String kafkaTopic, Map<String, List<Job>> map) {
    List<Job> jobs = map.get(kafkaTopic);
    if (jobs == null) {
      jobs = new ArrayList<>();
      map.put(kafkaTopic, jobs);
    }
    return jobs;
  }

  private void deleteJobFromMap(String kafkaTopic, long jobId, Map<String, List<Job>> map) {
    List<Job> jobs = map.get(kafkaTopic);
    if (jobs == null) {
      throw new VeniceException("Can not find kafka topic:" + kafkaTopic + " when deleting job:" + jobId);
    }
    boolean isFound = false;
    for (int i = 0; i < jobs.size(); i++) {
      if (jobs.get(i).getJobId() == jobId) {
        jobs.remove(i);
        isFound = true;
        break;
      }
    }
    if (!isFound) {
      throw new VeniceException("Can not find job:" + jobId);
    } else {
      if (jobs.isEmpty()) {
        map.remove(kafkaTopic);
      }
    }
  }

  private Job getJobFromMap(String kafkaTopic, long jobId, Map<String, List<Job>> map) {
    List<Job> jobs = map.get(kafkaTopic);
    if (jobs == null) {
      return null;
    }
    for (int i = 0; i < jobs.size(); i++) {
      if (jobs.get(i).getJobId() == jobId) {
        return jobs.get(i);
      }
    }
    return null;
  }

  public synchronized void clear() {
    this.topicToRunningJobsMap.clear();
    this.topicToTerminatedJobsMap.clear();
    this.adapter.unregisterSeralizer(offlineJobsPath);
    this.adapter.unregisterSeralizer(offlineJobsPath + "/");
    //We don't need to close ZK client here. It's could be reused by other repository.
  }

  private void updateJobPartitions(Job job, Map<Integer, Partition> partitions) {
    Set<Integer> changedPartitions = job.updateExecutingPartitions(partitions);
    if (!changedPartitions.isEmpty()) {
      for (Integer partitionId : changedPartitions) {
        try {
          this.updateTaskToZK(job.getJobId(), partitionId, job.tasksInPartition(partitionId));
        } catch (Throwable e) {
          // We don't need to break the whole update process here. Because even if the local copy is different from ZK,
          // it will be sync up again when task status is changed. The worst case is before next task status update
          // happening, controller is failed. But new controller will read job and tasks from zk and check the
          // partitions again which will also fix this problem.
          logger.error("Can not update tasks to ZK for job:" + job.getJobId() + " in partition:" + partitionId);
        }
      }
    }
  }

  @Override
  public synchronized void handleRoutingDataChange(String kafkaTopic, Map<Integer, Partition> partitions) {
    List<Job> jobs = this.getRunningJobOfTopic(kafkaTopic);
    if (jobs.size() > 1) {
      throw new VeniceException(
          "There should be only one job running for each kafka topic. But now there are:" + jobs.size()
              + " jobs running.");
    }
    Job job = jobs.get(0);
    synchronized (job) {
      try {
        updateJobPartitions(job, partitions);
        job.notify();
      } catch (VeniceException e) {
        logger.info("There are no enough partitions or replica to execute tasks.");
      }
    }
  }

  public RoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }
}
