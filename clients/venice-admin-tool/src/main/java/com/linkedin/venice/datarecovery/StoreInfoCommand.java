package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.security.SSLFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class StoreInfoCommand extends Command {
  StoreInfoCommand.Params params;
  StoreInfoCommand.Result result = new StoreInfoCommand.Result();

  public StoreInfoCommand(StoreInfoCommand.Params params) {
    this.params = params;
  }

  public StoreInfoCommand.Params getParams() {
    return this.params;
  }

  public StoreInfoCommand.Result getResult() {
    return this.result;
  }

  public void setParams(Params params) {
    this.params = params;
  }

  public void setResult(StoreInfoCommand.Result result) {
    this.result = result;
  }

  @Override
  public boolean needWaitForFirstTaskToComplete() {
    return false;
  }

  public ControllerClient buildControllerClient(String clusterName, String url, Optional<SSLFactory> sslFactory) {
    return new ControllerClient(clusterName, url, sslFactory);
  }

  @Override
  public void execute() {
    // get store's push + partition info
    String storeName = getParams().store;
    String clusterName = getParams().getPCtrlCliWithoutCluster().discoverCluster(storeName).getCluster();
    LocalDateTime timestamp = getParams().getTimestamp();
    String destFabric = getParams().getDestFabric();

    if (clusterName == null) {
      this.setResult(new StoreInfoCommand.Result(false, "unable to discover cluster for store", true));
    }
    boolean isBatch = false;
    String message = "OK";
    try (ControllerClient parentCtrlCli =
        buildControllerClient(clusterName, getParams().getUrl(), getParams().getSSLFactory())) {

      // check to see if the store is a batch store
      StoreResponse response = parentCtrlCli.getStore(storeName);

      if (response.isError()) {
        throw new Exception("store not found");
      }

      if (response.getStore().getHybridStoreConfig() == null) {
        isBatch = true;
      }

      // check if repush is possible
      StoreHealthAuditResponse storeHealthInfo = parentCtrlCli.listStorePushInfo(storeName, false);
      Map<String, RegionPushDetails> regionPushDetails = storeHealthInfo.getRegionPushDetails();
      if (!regionPushDetails.containsKey(destFabric)) {
        throw new Exception("nothing to repush -- store is on ver. 0");
      }

      // check timestamp
      String latestTimestamp = regionPushDetails.get(destFabric).getPushStartTimestamp();
      LocalDateTime latestPushStartTime = LocalDateTime.parse(latestTimestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);

      if (latestPushStartTime.isAfter(timestamp)) {
        throw new Exception("input timestamp earlier than latest push");
      }

      // check ongoing push
      MultiStoreStatusResponse futureVersionResponse = parentCtrlCli.getFutureVersions(clusterName, storeName);
      if (futureVersionResponse.getStoreStatusMap().containsKey(destFabric)
          && Integer.parseInt(futureVersionResponse.getStoreStatusMap().get(destFabric)) != 0) {
        throw new Exception(
            String.format(
                "push to version %d ongoing",
                Integer.parseInt(futureVersionResponse.getStoreStatusMap().get(destFabric))));
      } else if (Integer
          .parseInt(futureVersionResponse.getStoreStatusMap().get(destFabric)) != Store.NON_EXISTING_VERSION) {
        throw new Exception("offline push detected");
      } else {
        this.setResult(new StoreInfoCommand.Result(isBatch, message, false));
      }
    } catch (Exception e) {
      this.setResult(new StoreInfoCommand.Result(isBatch, e.getMessage(), true));
    }
    this.getResult().setCoreWorkDone(true);
  }

  public static class Params extends Command.Params {
    // command name.
    private String command;
    // dest fabric
    private String destFabric;
    // source fabric.
    private String sourceFabric;
    // extra arguments to command.
    private String extraCommandArgs;
    // expected completion timestamp
    private LocalDateTime timestamp;
    // Debug run.
    private boolean debug = false;

    private ControllerClient pCtrlCliWithoutCluster;
    private String url;
    private Optional<SSLFactory> sslFactory;

    public String getCommand() {
      return this.command;
    }

    public String getUrl() {
      return url;
    }

    public ControllerClient getPCtrlCliWithoutCluster() {
      return this.pCtrlCliWithoutCluster;
    }

    public LocalDateTime getTimestamp() {
      return this.timestamp;
    }

    public String getDestFabric() {
      return this.destFabric;
    }

    public Optional<SSLFactory> getSSLFactory() {
      return sslFactory;
    }

    public String getSourceFabric() {
      return sourceFabric;
    }

    public boolean getDebug() {
      return this.debug;
    }

    public static class Builder extends Command.Params.Builder {
      private String command;
      private String destFabric;
      private String sourceFabric;
      private String extraCommandArgs;
      private LocalDateTime timestamp;
      private ControllerClient pCtrlCliWithoutCluster;
      private String url;
      private Optional<SSLFactory> sslFactory;
      private boolean debug = false;

      public Builder() {
      }

      public Builder(
          String command,
          String destFabric,
          String sourceFabric,
          String extraCommandArgs,
          LocalDateTime timestamp,
          ControllerClient controllerClient,
          String url,
          Optional<SSLFactory> sslFactory,
          boolean debug) {
        this.setCommand(command)
            .setDestFabric(destFabric)
            .setSourceFabric(sourceFabric)
            .setExtraCommandArgs(extraCommandArgs)
            .setTimestamp(timestamp)
            .setPCtrlCliWithoutCluster(controllerClient)
            .setUrl(url)
            .setSSLFactory(sslFactory)
            .setDebug(debug);
      }

      public Builder(StoreInfoCommand.Params p) {
        this(
            p.command,
            p.destFabric,
            p.sourceFabric,
            p.extraCommandArgs,
            p.timestamp,
            p.pCtrlCliWithoutCluster,
            p.url,
            p.sslFactory,
            p.debug);
      }

      public Builder(StoreRepushCommand.Params p) {
        this(
            p.getCommand(),
            p.getDestFabric(),
            p.getSourceFabric(),
            p.getExtraCommandArgs(),
            p.getTimestamp(),
            p.getPCtrlCliWithoutCluster(),
            p.getUrl(),
            p.getSSLFactory(),
            p.getDebug());
      }

      @Override
      public StoreInfoCommand.Params build() {
        StoreInfoCommand.Params ret = new StoreInfoCommand.Params();
        ret.command = command;
        ret.destFabric = destFabric;
        ret.sourceFabric = sourceFabric;
        ret.extraCommandArgs = extraCommandArgs;
        ret.timestamp = timestamp;
        ret.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        ret.url = url;
        ret.sslFactory = sslFactory;
        ret.debug = debug;
        return ret;
      }

      @Override
      public List<DataRecoveryTask> buildTasks(Set<String> storeNames) {
        List<DataRecoveryTask> tasks = new ArrayList<>();
        for (String name: storeNames) {
          StoreInfoCommand.Params p = this.build();
          p.setStore(name);
          DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
          tasks.add(new DataRecoveryTask(new StoreInfoCommand(p), taskParams));
        }
        return tasks;
      }

      public StoreInfoCommand.Params.Builder setCommand(String command) {
        this.command = command;
        return this;
      }

      public StoreInfoCommand.Params.Builder setDestFabric(String destFabric) {
        this.destFabric = destFabric;
        return this;
      }

      public StoreInfoCommand.Params.Builder setSourceFabric(String sourceFabric) {
        this.sourceFabric = sourceFabric;
        return this;
      }

      public StoreInfoCommand.Params.Builder setExtraCommandArgs(String extraCommandArgs) {
        this.extraCommandArgs = extraCommandArgs;
        return this;
      }

      public StoreInfoCommand.Params.Builder setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public StoreInfoCommand.Params.Builder setPCtrlCliWithoutCluster(ControllerClient pCtrlCliWithoutCluster) {
        this.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        return this;
      }

      public StoreInfoCommand.Params.Builder setUrl(String url) {
        this.url = url;
        return this;
      }

      public StoreInfoCommand.Params.Builder setSSLFactory(Optional<SSLFactory> sslFactory) {
        this.sslFactory = sslFactory;
        return this;
      }

      public StoreInfoCommand.Params.Builder setDebug(boolean debug) {
        this.debug = debug;
        return this;
      }
    }
  }

  public class Result extends Command.Result {
    private boolean batchStore = false;
    private String message = null;
    private boolean error = false;

    public Result() {
    }

    public Result(boolean batchStore, String message, boolean error) {
      this.batchStore = batchStore;
      this.message = message;
      this.error = error;
    }

    public void setBatchStore(boolean batchStore) {
      this.batchStore = batchStore;
    }

    public void setError(boolean error) {
      this.error = error;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public boolean isBatchStore() {
      return batchStore;
    }

    public String getMessage() {
      return message;
    }

    public boolean isError() {
      return error;
    }
  }
}
