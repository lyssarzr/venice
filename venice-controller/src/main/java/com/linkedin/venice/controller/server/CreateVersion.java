package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion {
  private static final Logger logger = Logger.getLogger(CreateVersion.class);

  public static Route createVersionRoute(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, CREATE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        // TODO we should verify the data size at first. If it exceeds the quota, controller should reject this request.
        // TODO And also we should use quota to calculate partition count to avoid this case that data size of first
        // push is very small but grow dramatically because quota of this store is very large.
        // Store size in Bytes
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicaFactor = admin.getReplicationFactor(clusterName, storeName);
        Version version = admin.incrementVersion(clusterName, storeName, partitionNum, replicaFactor);
        // The actual partition number could be different from the one calculated here,
        // since Venice is not using dynamic partition number across different versions.
        responseObject.setPartitions(admin.getStore(clusterName, storeName).getPartitionCount());
        responseObject.setReplicas(replicaFactor);
        responseObject.setVersion(version.getNumber());
        responseObject.setKafkaTopic(version.kafkaTopicName());
        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers(isSSL));
        responseObject.setEnableSSL(isSSL);
        responseObject.setCompressionStrategy(version.getCompressionStrategy());
      } catch (Throwable e) {
        // TODO use the VeniceRouterHandler.handle
        if (e.getMessage() != null) {
          responseObject.setError(e.getMessage());
        } else {
          responseObject.setError(e.getClass().getName());
        }
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  /**
   * Instead of asking Venice to create a version, pushes should ask venice which topic to write into.
   * The logic below includes the ability to respond with an existing topic for the same push, allowing requests
   * to be idempotent
   *
   * @param admin
   * @return
   */
  public static Route requestTopicForPushing(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, REQUEST_TOPIC.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        Store store = admin.getStore(clusterName, storeName);
        if (null == store) {
          throw new VeniceNoStoreException(storeName);
        }
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        String pushTypeString = request.queryParams(PUSH_TYPE);
        PushType pushType;
        try {
          pushType = PushType.valueOf(pushTypeString);
        } catch (RuntimeException e){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, pushTypeString + " is an invalid " + PUSH_TYPE, e);
        }

        if (pushType.equals(PushType.STREAM) && !store.isHybrid()){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "requesting topic for streaming writes to store "
              + storeName + " which is not configured to be a hybrid store");
        }

        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        int partitionCount = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        responseObject.setReplicas(replicationFactor);
        responseObject.setPartitions(partitionCount);

        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers(isSSL));
        responseObject.setEnableSSL(isSSL);

        String pushJobId = request.queryParams(PUSH_JOB_ID);
        switch(pushType) {
          case BATCH:
          case INCREMENTAL:
            Version version;
            //TODO: merge the if-else brunch when we have idempotent topic query feature read
            if (pushType == PushType.BATCH) {
              version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionCount, replicationFactor, true);
            } else {
              version = admin.getIncrementalPushTopic(clusterName, storeName);
            }

            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(version.kafkaTopicName());
            responseObject.setCompressionStrategy(version.getCompressionStrategy());
            break;
          case STREAM:
            String realTimeTopic = admin.getRealTimeTopic(clusterName, storeName);
            responseObject.setKafkaTopic(realTimeTopic);
            break;
          default:
            throw new VeniceException(pushTypeString + " is an unrecognized " + PUSH_TYPE);
        }
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }

      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route uploadPushInfo(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, OFFLINE_PUSH_INFO.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        String versionString = request.queryParams(VERSION);
        int versionNumber = Integer.parseInt(versionString);
        Map<String, String> properties = new HashMap<>();
        for (String key : request.queryParams()) {
          properties.put(key, request.queryParams(key));
        }
        admin.updatePushProperties(clusterName, storeName, versionNumber, properties);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);

    };
  }

  public static Route writeEndOfPush(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, END_OF_PUSH.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String versionString = request.queryParams(VERSION);
        int versionNumber = Integer.parseInt(versionString);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        writeEndOfPush(admin, clusterName, storeName, versionNumber, false);

      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route emptyPush(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, EMPTY_PUSH.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        //Temporary fix until we can make #incrementVersionIdempotent work from the parent controller for versions beyond the first
        //Version version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionNum, replicationFactor, true);
        Version version = admin.incrementVersion(clusterName, storeName, partitionNum, replicationFactor); //TEMP
        int versionNumber = version.getNumber();

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        responseObject.setPartitions(partitionNum);
        responseObject.setReplicas(replicationFactor);

        writeEndOfPush(admin, clusterName, storeName, versionNumber, true);

        /** TODO: Poll {@link com.linkedin.venice.controller.VeniceParentHelixAdmin#getOffLineJobStatus(String, String, Map, TopicManager)} until it is terminal... */

      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  protected static void writeEndOfPush(Admin admin, String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
    //validate store and version exist
    Store store = admin.getStore(clusterName, storeName);

    if (null == store) {
      throw new VeniceNoStoreException(storeName);
    }

    if (store.getCurrentVersion() == versionNumber){
      throw new VeniceHttpException(HttpStatus.SC_CONFLICT, "Cannot end push for version " + versionNumber + " that is currently being served");
    }

    if (!store.containsVersion(versionNumber)){
      throw new VeniceHttpException(HttpStatus.SC_NOT_FOUND, "Version " + versionNumber + " was not found for Store " + storeName
          + ".  Cannot end push for version that does not exist");
    }

    //write EOP message
    try (VeniceWriter writer = admin.getVeniceWriterFactory()
        .getVeniceWriter(Version.composeKafkaTopic(storeName, versionNumber))) {
      if (alsoWriteStartOfPush) {
        writer.broadcastStartOfPush(new HashMap<>());
      }
      writer.broadcastEndOfPush(new HashMap<>());
    }
  }
}
