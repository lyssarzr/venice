package com.linkedin.venice;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.datarecovery.DataRecoveryClient;
import com.linkedin.venice.datarecovery.DataRecoveryExecutor;
import com.linkedin.venice.datarecovery.DataRecoveryTask;
import com.linkedin.venice.datarecovery.EstimateDataRecoveryTimeCommand;
import com.linkedin.venice.datarecovery.PlanningExecutor;
import com.linkedin.venice.datarecovery.PlanningTask;
import com.linkedin.venice.datarecovery.StoreRepushCommand;
import com.linkedin.venice.meta.PartitionDetail;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.ReplicaDetail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDataRecoveryClient {
  private DataRecoveryExecutor executor;
  private PlanningExecutor planningExecutor;
  private ControllerClient controllerClient;

  @Test
  public void testExecutor() {
    for (boolean isSuccess: new boolean[] { true, false }) {
      controllerClient = mock(ControllerClient.class);
      estimateRecovery();
      executeRecovery(isSuccess);
      verifyRecoveryResults(isSuccess);
    }
  }

  private void verifyRecoveryResults(boolean isSuccess) {
    int numOfStores = 3;
    Assert.assertEquals(executor.getTasks().size(), numOfStores);
    if (isSuccess) {
      // Verify all stores are executed successfully.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertFalse(executor.getTasks().get(i).getTaskResult().isError());
      }
    } else {
      // Verify all stores are executed unsuccessfully.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertTrue(executor.getTasks().get(i).getTaskResult().isError());
      }
    }
  }

  private void estimateRecovery() {
    EstimateDataRecoveryTimeCommand.Params cmdParams = new EstimateDataRecoveryTimeCommand.Params();
    planningExecutor = spy(PlanningExecutor.class);

    List<String> mockCmd = new ArrayList<>();
    mockCmd.add("sh");
    mockCmd.add("-c");

    EstimateDataRecoveryTimeCommand mockEstimateTimeCmd = spy(EstimateDataRecoveryTimeCommand.class);
    mockEstimateTimeCmd.setUrl("https://localhost:7036");
    doReturn(mockCmd).when(mockEstimateTimeCmd).getShellCmd();

    Set<String> storeNames = new HashSet<>(Arrays.asList("store1"));
    List<PlanningTask> tasks = buildPlanningTasks(storeNames, mockEstimateTimeCmd, cmdParams);
    doReturn(tasks).when(planningExecutor).buildTasks(any(), any(), any());
    DataRecoveryClient dataRecoveryClient = mock(DataRecoveryClient.class);
    doReturn(executor).when(dataRecoveryClient).getPlanningExecutor();
    doCallRealMethod().when(dataRecoveryClient).estimateRecoveryTime(any(), any(), any());

    StoreHealthAuditResponse mockResponse = new StoreHealthAuditResponse();
    List<ReplicaDetail> mockReplicaDetails = new ArrayList<ReplicaDetail>();
    for (int i = 0; i != 3; i++) {
      ReplicaDetail rep = new ReplicaDetail();
      rep.setPushStartDateTime("2023-03-09T00:20:15.063472");
      rep.setPushStartDateTime("2023-03-09T00:21:15.063472");
      mockReplicaDetails.add(rep);
    }
    List<ReplicaDetail> mockReplicaDetails2 = new ArrayList<ReplicaDetail>();
    for (int i = 0; i != 3; i++) {
      ReplicaDetail rep = new ReplicaDetail();
      rep.setPushStartDateTime("2023-03-09T00:20:15.063472");
      rep.setPushStartDateTime("2023-03-09T00:22:15.063472");
      mockReplicaDetails2.add(rep);
    }
    List<PartitionDetail> mockPartitionDetails = new ArrayList<PartitionDetail>();
    PartitionDetail a = new PartitionDetail();
    PartitionDetail b = new PartitionDetail();
    a.setReplicaDetails(mockReplicaDetails);
    b.setReplicaDetails(mockReplicaDetails2);

    List<PartitionDetail> partitionDetails = new ArrayList<PartitionDetail>();
    partitionDetails.add(a);
    partitionDetails.add(b);

    RegionPushDetails det = new RegionPushDetails();
    det.setPartitionDetails(partitionDetails);
    mockResponse.setRegionPushDetails(new HashMap<String, RegionPushDetails>() {
      {
        put("store1", det);
      }
    });

    // doReturn(mockResponse).when(controllerClient).listStorePushInfo(any(), any());
    dataRecoveryClient.estimateRecoveryTime(
        new DataRecoveryClient.DataRecoveryParams("store1"),
        controllerClient.getClusterName(),
        controllerClient);
  }

  private void executeRecovery(boolean isSuccess) {
    StoreRepushCommand.Params cmdParams = new StoreRepushCommand.Params();
    cmdParams.setCommand("cmd");
    cmdParams.setExtraCommandArgs("args");

    // Partial mock of Module class to take password from console input.
    executor = spy(DataRecoveryExecutor.class);
    doReturn("test").when(executor).getUserCredentials();

    // Mock command to mimic a successful repush result.
    List<String> mockCmd = new ArrayList<>();
    mockCmd.add("sh");
    mockCmd.add("-c");

    if (isSuccess) {
      mockCmd.add("echo \"success: https://example.com/executor?execid=21585379\"");
    } else {
      mockCmd.add("echo \"failure: Incorrect Login. Username/Password+VIP not found.\"");
    }
    StoreRepushCommand mockStoreRepushCmd = spy(StoreRepushCommand.class);
    mockStoreRepushCmd.setParams(cmdParams);
    doReturn(mockCmd).when(mockStoreRepushCmd).getShellCmd();

    // Inject the mocked command into the running system.
    Set<String> storeName = new HashSet<>(Arrays.asList("store1", "store2", "store3"));
    List<DataRecoveryTask> tasks = buildTasks(storeName, mockStoreRepushCmd, cmdParams);
    doReturn(tasks).when(executor).buildTasks(any(), any());

    // Partial mock of Client class to confirm to-be-repushed stores from standard input.
    DataRecoveryClient dataRecoveryClient = mock(DataRecoveryClient.class);
    doReturn(executor).when(dataRecoveryClient).getExecutor();
    doCallRealMethod().when(dataRecoveryClient).execute(any(), any());
    doReturn(true).when(dataRecoveryClient).confirmStores(any());
    // client executes three store recovery.
    dataRecoveryClient.execute(new DataRecoveryClient.DataRecoveryParams("store1,store2,store3"), cmdParams);
  }

  private List<DataRecoveryTask> buildTasks(
      Set<String> storeNames,
      StoreRepushCommand cmd,
      StoreRepushCommand.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, params);
      tasks.add(new DataRecoveryTask(cmd, taskParams));
    }
    return tasks;
  }

  private List<PlanningTask> buildPlanningTasks(
      Set<String> storeNames,
      EstimateDataRecoveryTimeCommand cmd,
      EstimateDataRecoveryTimeCommand.Params params) {
    List<PlanningTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      PlanningTask.TaskParams taskParams = new PlanningTask.TaskParams(name, params.getClusterName());
      tasks.add(new PlanningTask(taskParams, controllerClient));
    }
    return tasks;
  }
}
