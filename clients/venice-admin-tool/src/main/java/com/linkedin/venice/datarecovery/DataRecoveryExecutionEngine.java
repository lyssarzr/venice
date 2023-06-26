package com.linkedin.venice.datarecovery;

import static java.lang.Thread.*;

import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DataRecoveryExecutionEngine {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryExecutionEngine.class);
  private final static int DEFAULT_POOL_SIZE = 10;
  private final static int DEFAULT_POOL_TIMEOUT_IN_SECONDS = 30;
  public static final int INTERVAL_UNSET = -1;
  protected final int poolSize;
  protected int interval = INTERVAL_UNSET;
  protected final ExecutorService pool;

  protected Deque<DataRecoveryTaskGroup> taskGroups = new LinkedList<>();
  protected List<DataRecoveryTaskGroup> completedTasks = new ArrayList<>();
  protected List<DataRecoveryTask> currentTasks = new ArrayList<>();
  protected List<Pair<String, String>> failedTasks = new ArrayList<>();

  public DataRecoveryExecutionEngine() {
    this(DEFAULT_POOL_SIZE);
  }

  public DataRecoveryExecutionEngine(int poolSize) {
    this.poolSize = poolSize;
    this.pool = Executors.newFixedThreadPool(this.poolSize);
  }

  public List<Pair<String, String>> getFailedTasks() {
    return this.failedTasks;
  }

  public void estimateRecoveryTime(Set<String> storeNames, EstimateDataRecoveryTimeCommand.Params params) {
    taskGroups.add(buildTaskGroup(storeNames, params, DataRecoveryTaskGroup.Type.ESTIMATE_DATA_RECOVERY_TIME));
    perform();
  }

  public void monitor(Set<String> storeNames, MonitorCommand.Params params) {
    taskGroups.add(buildTaskGroup(storeNames, params, DataRecoveryTaskGroup.Type.MONITOR));
    perform();
  }

  public void executeStoreRepush(Set<String> storeNames, StoreRepushCommand.Params params) {
    StoreInfoCommand.Params.Builder b = new StoreInfoCommand.Params.Builder(params);
    taskGroups.addFirst(buildTaskGroup(storeNames, b.build(), DataRecoveryTaskGroup.Type.STORE_INFO));
    perform();

    Set<String> batchStores = new HashSet<>();
    Set<String> hybridStores = new HashSet<>();

    LOGGER.info(completedTasks.get(0).getTasks().size());
    List<DataRecoveryTask> tasks = currentTasks;
    for (int i = 0; i != tasks.size(); i++) {
      String storeName = tasks.get(i).getTaskParams().getStore();
      LOGGER.info(storeName);
      StoreInfoCommand.Result result = (StoreInfoCommand.Result) tasks.get(i).getTaskResult().getCmdResult();
      if (result.isError()) {
        failedTasks.add(Pair.of(storeName, result.getMessage()));
      } else {
        if (result.isBatchStore()) {
          batchStores.add(storeName);
        } else {
          hybridStores.add(storeName);
        }
      }
    }

    if (!batchStores.isEmpty()) {
      LOGGER.info("Recovering " + batchStores.size() + " batch stores");
      BatchStoreDataRecoveryCommand.Params.Builder batchBuilder =
          new BatchStoreDataRecoveryCommand.Params.Builder(params);
      taskGroups.addLast(
          buildTaskGroup(batchStores, batchBuilder.build(), DataRecoveryTaskGroup.Type.BATCH_STORE_DATA_RECOVERY));
    }
    if (!hybridStores.isEmpty()) {
      taskGroups.addFirst(buildTaskGroup(hybridStores, params, DataRecoveryTaskGroup.Type.STORE_REPUSH));
    }
    if (!taskGroups.isEmpty()) {
      perform();
    } else {
      LOGGER.info("No stores were recovered.");
    }
  }

  public DataRecoveryTaskGroup buildTaskGroup(
      Set<String> storeNames,
      Command.Params params,
      DataRecoveryTaskGroup.Type type) {
    List<DataRecoveryTask> tasks = null;
    switch (type) {
      case ESTIMATE_DATA_RECOVERY_TIME:
        EstimateDataRecoveryTimeCommand.Params estimateCmdParams = (EstimateDataRecoveryTimeCommand.Params) params;
        EstimateDataRecoveryTimeCommand.Params.Builder estimateBuilder =
            new EstimateDataRecoveryTimeCommand.Params.Builder(estimateCmdParams);
        tasks = estimateBuilder.buildTasks(storeNames);
        LOGGER.info("Built command of type " + type.toString());
        break;
      case STORE_REPUSH:
        StoreRepushCommand.Params repushCmdParams = (StoreRepushCommand.Params) params;
        StoreRepushCommand.Params.Builder repushBuilder = new StoreRepushCommand.Params.Builder(repushCmdParams);
        tasks = repushBuilder.buildTasks(storeNames);
        LOGGER.info("Built command of type " + type.toString());
        break;
      case STORE_INFO:
        StoreInfoCommand.Params infoCmdParams = (StoreInfoCommand.Params) params;
        StoreInfoCommand.Params.Builder infoBuilder = new StoreInfoCommand.Params.Builder(infoCmdParams);
        tasks = infoBuilder.buildTasks(storeNames);
        LOGGER.info("Built command of type " + type.toString());
        break;
      case MONITOR:
        MonitorCommand.Params monitorCmdParams = (MonitorCommand.Params) params;
        MonitorCommand.Params.Builder monitorBuilder = new MonitorCommand.Params.Builder(monitorCmdParams);
        tasks = monitorBuilder.buildTasks(storeNames);
        LOGGER.info("Built command of type " + type.toString());
        break;
      case BATCH_STORE_DATA_RECOVERY:
        BatchStoreDataRecoveryCommand.Params batchCmdParams = (BatchStoreDataRecoveryCommand.Params) params;
        BatchStoreDataRecoveryCommand.Params.Builder batchBuilder =
            new BatchStoreDataRecoveryCommand.Params.Builder(batchCmdParams);
        tasks = batchBuilder.buildTasks(storeNames);
        LOGGER.info("Built command of type " + type.toString());
        break;
    }
    return new DataRecoveryTaskGroup(type, tasks);
  }

  void displayTaskResult(DataRecoveryTask task) {
    // print things here or something
  }

  public Deque<DataRecoveryTaskGroup> getTaskGroups() {
    return taskGroups;
  }

  /**
   * For some task, it is benefit to wait for the first task to complete before starting to run the remaining ones.
   * e.g. the first run of task can set up local session files that can be used by follow-up tasks.
   */
  public boolean needWaitForFirstTaskToComplete(DataRecoveryTask task) {
    return task.needWaitForFirstTaskToComplete();
  }

  public void perform() {
    Iterator<DataRecoveryTaskGroup> it = taskGroups.descendingIterator();
    while (it.hasNext()) {
      DataRecoveryTaskGroup taskGroup = it.next();
      currentTasks = taskGroup.getTasks();
      if (currentTasks.isEmpty()) {
        return;
      }

      List<DataRecoveryTask> concurrentTasks = currentTasks;
      DataRecoveryTask firstTask = currentTasks.get(0);
      if (needWaitForFirstTaskToComplete(firstTask)) {
        // Let the main thread run the first task to completion if there is a need.
        firstTask.run();
        if (firstTask.getTaskResult().isError()) {
          displayTaskResult(firstTask);
          return;
        }
        // Exclude the 1st item from the list as it has finished.
        concurrentTasks = currentTasks.subList(1, currentTasks.size());
      }

      /*
       * Keep polling the states (for monitor) of all tasks at given intervals when interval is set to certain value
       * plus not all tasks are finished. Otherwise, if interval is unset, just do a one time execution for all tasks.
       */
      do {
        try (Timer ignored = Timer.run(elapsedTimeInMs -> {
          if (continuePollingState()) {
            Utils.sleep(computeTimeToSleepInMillis(elapsedTimeInMs));
          }
        })) {
          List<CompletableFuture<Void>> taskFutures = concurrentTasks.stream()
              .map(dataRecoveryTask -> CompletableFuture.runAsync(dataRecoveryTask, pool))
              .collect(Collectors.toList());
          taskFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());

          for (CompletableFuture<Void> future: taskFutures) {
            future.join();
          }
          processData();
          displayAllTasksResult();
        }
      } while (continuePollingState());
      completedTasks.add(taskGroup);
      it.remove();
    }
    shutdownAndAwaitTermination();
  }

  public void processData() {
  }

  private void displayAllTasksResult() {
    int numDoneTasks = 0;
    int numSuccessfullyDoneTasks = 0;

    for (DataRecoveryTask dataRecoveryTask: currentTasks) {
      displayTaskResult(dataRecoveryTask);
      if (dataRecoveryTask.getTaskResult().isCoreWorkDone()) {
        numDoneTasks++;
        if (!dataRecoveryTask.getTaskResult().isError()) {
          numSuccessfullyDoneTasks++;
        }
      }
    }
    LOGGER.info(
        "Total: {}, Succeeded: {}, Error: {}, Uncompleted: {}",
        currentTasks.size(),
        numSuccessfullyDoneTasks,
        numDoneTasks - numSuccessfullyDoneTasks,
        currentTasks.size() - numDoneTasks);
  }

  private boolean continuePollingState() {
    return isIntervalSet() && !areAllCoreWorkDone();
  }

  private boolean isIntervalSet() {
    return interval != INTERVAL_UNSET;
  }

  private boolean areAllCoreWorkDone() {
    for (DataRecoveryTask task: currentTasks) {
      if (!task.getTaskResult().isCoreWorkDone()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Calculate the sleep time based on the interval setting and the latency that has already occurred.
   */
  private long computeTimeToSleepInMillis(double latency) {
    long sleepTime = TimeUnit.SECONDS.toMillis(interval) - (long) latency;
    return sleepTime > 0 ? sleepTime : 0;
  }

  public void shutdownAndAwaitTermination() {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(DEFAULT_POOL_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
        // Cancel currently executing tasks.
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }
}
