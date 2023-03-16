package com.linkedin.venice.datarecovery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * EstimateRecoveryTimeCommand contains the details of a request for estimating the recovery time of a store.
 * We expect the command to comply with the following contract:
 *
 * Input:
 *    admin-tool.sh --list-store-push-info --url <url>--store <store_name> --cluster <source_fabric>
 * Output:
 *    success: link_to_running_task
 *    failure: failure_reason
 */

public class EstimateDataRecoveryTimeCommand {
  private static final Logger LOGGER = LogManager.getLogger(EstimateDataRecoveryTimeCommand.class);

  // Store name.
  private String store;
  private EstimateDataRecoveryTimeCommand.Params params;
  private Result result;
  private List<String> shellCmd;

  // For unit test only.
  public EstimateDataRecoveryTimeCommand() {
  }

  public EstimateDataRecoveryTimeCommand(String store, EstimateDataRecoveryTimeCommand.Params params, String url) {
    this.store = store;
    this.params = params;
    this.shellCmd = generateShellCmd(generateListStorePushInfoCommand());
  }

  // For unit test only.

  public String getStore() {
    return store;
  }

  public Result getResult() {
    return result;
  }

  public String getUrl() {
    return this.params.getParentUrl();
  }

  public void setUrl(String url) {
    if (this.params == null)
      this.params = new EstimateDataRecoveryTimeCommand.Params();
    this.params.setParentUrl(url);
  }

  private List<String> generateListStorePushInfoCommand() {
    List<String> cmd = new ArrayList<>();
    cmd.add(String.format("--url '%s'", this.params.getParentUrl()));
    cmd.add(String.format("--cluster '%s'", this.params.getClusterName()));
    cmd.add(String.format("--store '%s'", this.store));
    return cmd;
  }

  private List<String> generateShellCmd(List<String> cmd) {
    List<String> shellCmd = new ArrayList<>();
    // Start a shell process so that it contains the right PATH variables.
    shellCmd.add("sh");
    shellCmd.add("-c");
    shellCmd.add(String.join(" ", cmd));
    return shellCmd;
  }

  public List<String> getShellCmd() {
    if (shellCmd == null) {
      shellCmd = generateShellCmd(generateListStorePushInfoCommand());
    }
    return shellCmd;
  }

  public void processOutput(String output, int exitCode) {
    result = new Result();
    result.setStdOut(output);
    result.setExitCode(exitCode);
    result.parseStandardOutput();
  }

  public void execute() {
    ProcessBuilder pb = new ProcessBuilder(getShellCmd());
    // so we can ignore the error stream.
    pb.redirectErrorStream(true);
    int exitCode = -1;
    BufferedReader reader = null;
    String stdOut = StringUtils.EMPTY;
    try {
      Process process = pb.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      StringBuilder buf = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        buf.append(line);
        buf.append('\n');
      }
      stdOut = buf.toString();
      // remove trailing white spaces and new lines.
      stdOut = stdOut.trim();

      exitCode = process.waitFor();
      processOutput(stdOut, exitCode);
    } catch (IOException e) {
      LOGGER.error("Error in executing command: {}", this, e);
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted when waiting for executing command: {}", this, e);
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOGGER.error("Error in closing reader for command: {}", this, e);
      }
    }

    if (params.debug) {
      LOGGER.info("Cmd: {}, StdOut: {}, Exit code: {}", this, stdOut, exitCode);
    }
  }

  @Override
  public String toString() {
    String cmd = "StoreRepushCommand{\n" + String.join(" ", shellCmd) + "\n}";
    return cmd;
  }

  public static class Params {
    private String clusterName;
    private String parentUrl;
    private boolean debug = false;

    public String getClusterName() {
      return clusterName;
    }

    public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
    }

    public String getParentUrl() {
      return parentUrl;
    }

    public void setParentUrl(String parentUrl) {
      this.parentUrl = parentUrl;
    }

    public void setDebug(boolean debug) {
      this.debug = debug;
    }
  }

  public static class Result {
    private Integer estimatedRecoveryTimeInSeconds;
    private String stdOut;
    private int exitCode;
    private String error;
    private String message;

    public int getExitCode() {
      return exitCode;
    }

    public void setExitCode(int exitCode) {
      this.exitCode = exitCode;
    }

    public String getStdOut() {
      return stdOut;
    }

    public void setStdOut(String stdOut) {
      this.stdOut = stdOut;
    }

    public boolean isError() {
      return error != null;
    }

    public void setError(String error) {
      this.error = error;
    }

    public String getError() {
      return error;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public void parseStandardOutput() {
      // No standard output or empty output.
      if (stdOut == null || stdOut.equals(StringUtils.EMPTY)) {
        return;
      }

      // Command reached to Azkaban, no matter it was a success or a failure.
      if (matchSuccessPattern()) {
        return;
      }

      if (matchFailurePattern()) {
        return;
      }

      // Failed: repush command itself hit an error (e.g. incomplete parameters)
      error = stdOut;
    }

    private boolean matchSuccessPattern() {
      // success: https://example.com/executor?execid=21585379
      String successPattern = "^success: (.*)$";
      Pattern pattern = Pattern.compile(successPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      Matcher matcher = pattern.matcher(stdOut);
      if (matcher.find()) {
        message = matcher.group();
        return true;
      }
      return false;
    }

    private boolean matchFailurePattern() {
      // failure: invalid password
      String errorPattern = "^failure: (.*)$";
      Pattern pattern = Pattern.compile(errorPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      Matcher matcher = pattern.matcher(stdOut);
      // Find the first occurrence of an error line and report.
      if (matcher.find()) {
        error = matcher.group();
        return true;
      }
      return false;
    }
  }
}
