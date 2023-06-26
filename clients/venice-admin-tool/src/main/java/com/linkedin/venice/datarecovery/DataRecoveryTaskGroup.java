package com.linkedin.venice.datarecovery;

import java.util.List;


public class DataRecoveryTaskGroup {
  public enum Type {
    STORE_REPUSH, ESTIMATE_DATA_RECOVERY_TIME, MONITOR, STORE_INFO, BATCH_STORE_DATA_RECOVERY
  }

  private List<DataRecoveryTask> tasks;
  private Type type;

  public DataRecoveryTaskGroup() {
  }

  public DataRecoveryTaskGroup(DataRecoveryTaskGroup.Type type, List<DataRecoveryTask> tasks) {
    this.type = type;
    this.tasks = tasks;
  }

  public Type getType() {
    return this.type;
  }

  public List<DataRecoveryTask> getTasks() {
    return this.tasks;
  }
}
