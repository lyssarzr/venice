package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;


public class BatchStoreDataRecoveryCommand extends Command {
  private BatchStoreDataRecoveryCommand.Result result = new BatchStoreDataRecoveryCommand.Result();
  private BatchStoreDataRecoveryCommand.Params params = new BatchStoreDataRecoveryCommand.Params();

  public BatchStoreDataRecoveryCommand(BatchStoreDataRecoveryCommand.Params p) {
    this.params = p;
  }

  public void setParams(Params p) {
    this.params = p;
  }

  public void setResult(Result r) {
    this.result = r;
  }

  public BatchStoreDataRecoveryCommand.Result getResult() {
    return this.result;
  }

  public BatchStoreDataRecoveryCommand.Params getParams() {
    return this.params;
  }

  @Override
  public boolean needWaitForFirstTaskToComplete() {
    return true;
  }

  public ControllerClient buildControllerClient(String clusterName, String url, Optional<SSLFactory> sslFactory) {
    return new ControllerClient(clusterName, url, sslFactory);
  }

  @Override
  public void execute() {
    String storeName = getParams().store;
    String clusterName = getParams().getPCtrlCliWithoutCluster().discoverCluster(storeName).getCluster();

    try (ControllerClient parentCtrlCli =
        buildControllerClient(clusterName, getParams().getUrl(), getParams().getSSLFactory())) {
      parentCtrlCli.prepareDataRecovery(
          getParams().getSourceFabric(),
          getParams().getDestFabric(),
          storeName,
          -1,
          Optional.empty());
      parentCtrlCli.dataRecovery(
          getParams().getSourceFabric(),
          getParams().getDestFabric(),
          storeName,
          -1,
          false,
          true,
          Optional.empty());
    }
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

      public Builder(BatchStoreDataRecoveryCommand.Params p) {
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
      public BatchStoreDataRecoveryCommand.Params build() {
        BatchStoreDataRecoveryCommand.Params ret = new BatchStoreDataRecoveryCommand.Params();
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
          BatchStoreDataRecoveryCommand.Params p = this.build();
          p.setStore(name);
          DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
          tasks.add(new DataRecoveryTask(new BatchStoreDataRecoveryCommand(p), taskParams));
        }
        return tasks;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setCommand(String command) {
        this.command = command;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setDestFabric(String destFabric) {
        this.destFabric = destFabric;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setSourceFabric(String sourceFabric) {
        this.sourceFabric = sourceFabric;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setExtraCommandArgs(String extraCommandArgs) {
        this.extraCommandArgs = extraCommandArgs;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setPCtrlCliWithoutCluster(
          ControllerClient pCtrlCliWithoutCluster) {
        this.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setUrl(String url) {
        this.url = url;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setSSLFactory(Optional<SSLFactory> sslFactory) {
        this.sslFactory = sslFactory;
        return this;
      }

      public BatchStoreDataRecoveryCommand.Params.Builder setDebug(boolean debug) {
        this.debug = debug;
        return this;
      }
    }
  }

  public static class Result extends Command.Result {
    public boolean error = false;
    public String message = "";

    public Result() {
    }

    public Result(boolean error, String message) {
      this.error = error;
      this.message = message;
    }

    public boolean isError() {
      return error;
    }

    public String getMessage() {
      return message;
    }
  }
}
