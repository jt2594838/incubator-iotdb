/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.service;

import static org.apache.iotdb.db.conf.IoTDBConstant.PRIVILEGE;
import static org.apache.iotdb.db.conf.IoTDBConstant.ROLE;
import static org.apache.iotdb.db.conf.IoTDBConstant.USER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.DatabaseEngineFactory;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.ConsistencyException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.Metadata;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSHandleIdentifier;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thrift RPC implementation at server side.
 */

public class TSServiceImpl implements TSIService.Iface, ServerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);
  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private static final String ERROR_NOT_LOGIN = "Not login";
  private static final String ERROR_QUERY_IN_BATCH = "statement is query";

  protected QueryProcessor processor;
  // Record the username for every rpc connection. Username.get() is null if
  // login is failed.
  protected ThreadLocal<String> username = new ThreadLocal<>();
  private ThreadLocal<HashMap<String, PhysicalPlan>> queryStatus = new ThreadLocal<>();
  private ThreadLocal<HashMap<String, QueryDataSet>> queryRet = new ThreadLocal<>();
  private ThreadLocal<ZoneId> zoneIds = new ThreadLocal<>();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private ThreadLocal<Map<Long, QueryContext>> contextMapLocal = new ThreadLocal<>();

  public TSServiceImpl() {
    processor = new QueryProcessor(new OverflowQPExecutor());
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    LOGGER.info("{}: receive open session request from username {}", IoTDBConstant.GLOBAL_DB_NAME,
        req.getUsername());

    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    try {
      status = authorizer.login(req.getUsername(), req.getPassword());
    } catch (AuthException e) {
      LOGGER.error("meet error while logging in.", e);
      status = false;
    }
    TS_Status tsStatus;
    if (status) {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      tsStatus.setErrorMessage("login successfully.");
      username.set(req.getUsername());
      zoneIds.set(config.getZoneID());
      initForOneSession();
    } else {
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage("login failed. Username or password is wrong.");
    }
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
        TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1);
    resp.setSessionHandle(
        new TS_SessionHandle(new TSHandleIdentifier(ByteBuffer.wrap(req.getUsername().getBytes()),
            ByteBuffer.wrap(req.getPassword().getBytes()))));
    LOGGER.info("{}: Login status: {}. User : {}", IoTDBConstant.GLOBAL_DB_NAME,
        tsStatus.getErrorMessage(),
        req.getUsername());

    return resp;
  }

  private void initForOneSession() {
    queryStatus.set(new HashMap<>());
    queryRet.set(new HashMap<>());
  }

  @Override
  public TSCloseSessionResp closeSession(TSCloseSessionReq req) {
    LOGGER.info("{}: receive close session", IoTDBConstant.GLOBAL_DB_NAME);
    TS_Status tsStatus;
    if (username.get() == null) {
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage("Has not logged in");
      if (zoneIds.get() != null) {
        zoneIds.remove();
      }
    } else {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      username.remove();
      if (zoneIds.get() != null) {
        zoneIds.remove();
      }
    }
    return new TSCloseSessionResp(tsStatus);
  }

  @Override
  public TSCancelOperationResp cancelOperation(TSCancelOperationReq req) {
    return new TSCancelOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSCloseOperationResp closeOperation(TSCloseOperationReq req) {
    LOGGER.info("{}: receive close operation", IoTDBConstant.GLOBAL_DB_NAME);
    try {

      releaseQueryResource(req);

      clearAllStatusForCurrentRequest();
    } catch (StorageGroupManagerException e) {
      LOGGER.error("Error in closeOperation : ", e);
    }
    return new TSCloseOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
  }

  private void releaseQueryResource(TSCloseOperationReq req) throws StorageGroupManagerException {
    Map<Long, QueryContext> contextMap = contextMapLocal.get();
    if (contextMap == null) {
      return;
    }
    if (req == null || req.queryId == -1) {
      // end query for all the query tokens created by current thread
      for (QueryContext context : contextMap.values()) {
        QueryResourceManager.getInstance().endQueryForGivenJob(context.getJobId());
      }
      contextMapLocal.set(new HashMap<>());
    } else {
      QueryResourceManager.getInstance()
          .endQueryForGivenJob(contextMap.remove(req.queryId).getJobId());
    }
  }

  private void clearAllStatusForCurrentRequest() {
    if (this.queryRet.get() != null) {
      this.queryRet.get().clear();
    }
    if (this.queryStatus.get() != null) {
      this.queryStatus.get().clear();
    }
  }

  private TS_Status getErrorStatus(String message) {
    TS_Status status = new TS_Status(TS_StatusCode.ERROR_STATUS);
    status.setErrorMessage(message);
    return status;
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TS_Status status;
    if (!checkLogin()) {
      LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = getErrorStatus(ERROR_NOT_LOGIN);
      return new TSFetchMetadataResp(status);
    }
    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    try {
      switch (req.getType()) {
        case "SHOW_TIMESERIES":
          String path = req.getColumnPath();
          List<List<String>> showTimeseriesList = getTimeSeriesForPath(path);
          resp.setShowTimeseriesList(showTimeseriesList);
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "SHOW_STORAGE_GROUP":
          Set<String> storageGroups = getAllStorageGroups();
          resp.setShowStorageGroups(storageGroups);
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "METADATA_IN_JSON":
          String metadataInJson;
          metadataInJson = getMetadataInString();
          resp.setMetadataInJson(metadataInJson);
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "DELTA_OBJECT":
          Metadata metadata;
          String column = req.getColumnPath();
          metadata = getMetadata();
          Map<String, List<String>> deviceMap = metadata.getDeviceMap();
          if (deviceMap == null || !deviceMap.containsKey(column)) {
            resp.setColumnsList(new ArrayList<>());
          } else {
            resp.setColumnsList(deviceMap.get(column));
          }
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "COLUMN":
          resp.setDataType(getSeriesType(req.getColumnPath()).toString());
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "ALL_COLUMNS":
          resp.setColumnsList(getPaths(req.getColumnPath()));
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        default:
          status = new TS_Status(TS_StatusCode.ERROR_STATUS);
          status
              .setErrorMessage(String.format("Unsupported fetch metadata operation %s",
                  req.getType()));
          break;
      }
    } catch (PathErrorException e) {
      LOGGER.error("cannot get metadata for {} ", req.getType(), e);
      status = getErrorStatus(String.format("cannot get metadata for %s because: %s",
          req.getType(), e));
      resp.setStatus(status);
      return resp;
    } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
      LOGGER.error("Failed to get delta object map", outOfMemoryError);
      status = getErrorStatus(
          String.format("Failed to get delta object map because: %s", outOfMemoryError));
    }
    resp.setStatus(status);
    return resp;
  }

  private Set<String> getAllStorageGroups() throws PathErrorException {
    return MManager.getInstance().getAllStorageGroup();
  }

  private List<List<String>> getTimeSeriesForPath(String path)
      throws PathErrorException {
    return MManager.getInstance().getShowTimeseriesPath(path);
  }

  private String getMetadataInString() {
    return MManager.getInstance().getMetadataInString();
  }

  protected Metadata getMetadata()
      throws PathErrorException {
    return MManager.getInstance().getMetadata();
  }

  protected TSDataType getSeriesType(String path)
      throws PathErrorException {
    return MManager.getInstance().getSeriesType(path);
  }

  protected List<String> getPaths(String path)
      throws PathErrorException {
    return MManager.getInstance().getPaths(path);
  }

  /**
   * Judge whether the statement is ADMIN COMMAND and if true, execute it.
   *
   * @param statement command
   * @return true if the statement is ADMIN COMMAND
   * @throws IOException exception
   */
  private boolean execAdminCommand(String statement) throws IOException {
    if (!"root".equals(username.get())) {
      return false;
    }
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase();
    switch (statement) {
      case "flush":
        try {
          DatabaseEngineFactory.getCurrent().closeAll();
        } catch (StorageGroupManagerException e) {
          LOGGER.error("meet error while DataBaseEngine closing all!", e);
          throw new IOException(e);
        }
        return true;
      case "merge":
        try {
          DatabaseEngineFactory.getCurrent().mergeAll();
        } catch (StorageGroupManagerException e) {
          LOGGER.error("meet error while DataBaseEngine merging all!", e);
          throw new IOException(e);
        }
        return true;
      default:
        return false;
    }
  }

  @Override
  public TSExecuteBatchStatementResp executeBatchStatement(TSExecuteBatchStatementReq req) {
    try {
      if (!checkLogin()) {
        LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN, null);
      }
      List<String> statements = req.getStatements();
      List<Integer> result = new ArrayList<>();
      boolean isAllSuccessful = true;
      String batchErrorMessage = "";

      for (String statement : statements) {
        String msg = executeOneStatementInBatch(statement, result);
        if (ERROR_QUERY_IN_BATCH.equals(msg)) {
          return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
              "statement is query :" + statement, result);
        }
        isAllSuccessful = isAllSuccessful && msg != null;
        batchErrorMessage = msg;
      }
      if (isAllSuccessful) {
        return getTSBathExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS,
            "Execute batch statements successfully", result);
      } else {
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, batchErrorMessage, result);
      }
    } catch (Exception e) {
      LOGGER.error("{}: error occurs when executing statements", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage(), null);
    }
  }

  private String executeOneStatementInBatch(String statement, List<Integer> result) {
    String batchErrorMessage = null;
    try {
      PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      physicalPlan.setProposer(username.get());
      if (physicalPlan.isQuery()) {
        return ERROR_QUERY_IN_BATCH;
      }
      TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan);
      if (resp.getStatus().getStatusCode().equals(TS_StatusCode.SUCCESS_STATUS)) {
        result.add(Statement.SUCCESS_NO_INFO);
      } else {
        result.add(Statement.EXECUTE_FAILED);
        batchErrorMessage = resp.getStatus().getErrorMessage();
      }
    } catch (Exception e) {
      String errMessage = String.format(
          "Fail to generate physical plan and execute for statement "
              + "%s because %s",
          statement, e.getMessage());
      LOGGER.warn("Error occurred when executing {}", statement, e);
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage = errMessage;
    }
    return batchErrorMessage;
  }

  @Override
  @SuppressWarnings("squid:2583")
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
      if (!checkLogin()) {
        LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
      }
      String statement = req.getStatement();

      try {
        if (execAdminCommand(statement)) {
          return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "ADMIN_COMMAND_SUCCESS");
        }
      } catch (Exception e) {
        LOGGER.error("meet error while executing admin command!", e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
      }

      try {
        if (execSetConsistencyLevel(statement)) {
          return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS,
              "Execute set consistency level successfully");
        }
      } catch (ConsistencyException e) {
        LOGGER.error("Error occurred when executing statement {}", statement, e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
      }

      PhysicalPlan physicalPlan;
      try {
        physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
        physicalPlan.setProposer(username.get());
      } catch (QueryProcessorException | ArgsErrorException | ProcessorException |
          NullPointerException e) {
        LOGGER.debug("meet error while parsing SQL to physical plan: ", e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "Statement format is not right:" + e.getMessage());
      }

      if (physicalPlan.isQuery()) {
        return executeQueryStatement(req);
      } else {
        return executeUpdateStatement(physicalPlan);
      }
  }

  /**
   * Set consistency level
   */
  private boolean execSetConsistencyLevel(String statement) throws ConsistencyException {
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase().trim();
    if (Pattern.matches(IoTDBConstant.SET_READ_CONSISTENCY_LEVEL_PATTERN, statement)) {
      throw new ConsistencyException(
          "IoTDB Stand-alone version does not support setting read-write consistency level");
    } else {
      return false;
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {

    try {
      if (!checkLogin()) {
        LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
      }

      String statement = req.getStatement();
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      plan.setProposer(username.get());

      TSExecuteStatementResp resp;
      List<String> columns = new ArrayList<>();
      if (!(plan instanceof AuthorPlan)) {
        resp = executeDataQuery(plan, columns);
      } else {
        resp = executeAuthQuery(plan, columns);
      }

      resp.setOperationType(plan.getOperatorType().toString());
      TSHandleIdentifier operationId = new TSHandleIdentifier(
          ByteBuffer.wrap(username.get().getBytes()),
          ByteBuffer.wrap("PASS".getBytes()));
      TSOperationHandle operationHandle;
      resp.setColumns(columns);
      operationHandle = new TSOperationHandle(operationId, true);
      resp.setOperationHandle(operationHandle);
      recordANewQuery(statement, plan);
      return resp;
    } catch (Exception e) {
      LOGGER.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  private TSExecuteStatementResp executeAuthQuery(PhysicalPlan plan, List<String> columns) {
    TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
    resp.setIgnoreTimeStamp(true);
    AuthorPlan authorPlan = (AuthorPlan) plan;
    switch (authorPlan.getAuthorType()) {
      case LIST_ROLE:
        columns.add(ROLE);
        break;
      case LIST_USER:
        columns.add(USER);
        break;
      case LIST_ROLE_USERS:
        columns.add(USER);
        break;
      case LIST_USER_ROLES:
        columns.add(ROLE);
        break;
      case LIST_ROLE_PRIVILEGE:
        columns.add(PRIVILEGE);
        break;
      case LIST_USER_PRIVILEGE:
        columns.add(ROLE);
        columns.add(PRIVILEGE);
        break;
      default:
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, String.format("%s is not an "
            + "auth query", authorPlan.getAuthorType()));
    }
    return resp;
  }

  private TSExecuteStatementResp executeDataQuery(PhysicalPlan plan, List<String> columns)
      throws TException {
    List<Path> paths;
    paths = plan.getPaths();

    // check seriesPath exists
    if (paths.isEmpty()) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Timeseries does not exist.");
    }

    // check file level set
    try {
      checkFileLevelSet(paths);
    } catch (PathErrorException e) {
      LOGGER.error("meet error while checking file level.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    // check permissions
    try {
      if (!checkAuthorization(paths, plan)) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "No permissions for this query.");
      }
    } catch (AuthException e) {
      LOGGER.error("meet error in authorization", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Authorization error " + e.getMessage());
    }

    TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    collectColumns(plan, paths, columns);
    return resp;
  }

  private void collectColumns(PhysicalPlan plan, List<Path> paths, List<String> columns) throws TException {
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (Path p : paths) {
          columns.add(p.getFullPath());
        }
        break;
      case AGGREGATION:
      case GROUPBY:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (int i = 0; i < paths.size(); i++) {
          columns.add(aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")");
        }
        break;
      default:
        throw new TException("unsupported query type: " + plan.getOperatorType());
    }
  }

  private void checkFileLevelSet(List<Path> paths) throws PathErrorException {
    MManager.getInstance().checkFileLevel(paths);
  }

  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin()) {
        return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Not login.");
      }
      String statement = req.getStatement();

      if (!queryStatus.get().containsKey(statement)) {
        return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Has not executed statement");
      }

      int fetchSize = req.getFetch_size();
      QueryDataSet queryDataSet;
      if (!queryRet.get().containsKey(statement)) {
        queryDataSet = createNewDataSet(statement, fetchSize, req);
      } else {
        queryDataSet = queryRet.get().get(statement);
      }
      TSQueryDataSet result = Utils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize);
      boolean hasResultSet = !result.getRecords().isEmpty();
      if (!hasResultSet && queryRet.get() != null) {
        queryRet.get().remove(statement);
      }
      TSFetchResultsResp resp = getTSFetchResultsResp(TS_StatusCode.SUCCESS_STATUS,
          "FetchResult successfully. Has more result: " + hasResultSet);
      resp.setHasResultSet(hasResultSet);
      resp.setQueryDataSet(result);
      return resp;
    } catch (Exception e) {
      LOGGER.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  private QueryDataSet createNewDataSet(String statement, int fetchSize, TSFetchResultsReq req)
      throws PathErrorException, QueryFilterOptimizationException, StorageGroupManagerException,
      ProcessorException, IOException {
    PhysicalPlan physicalPlan = queryStatus.get().get(statement);
    processor.getExecutor().setFetchSize(fetchSize);

    QueryDataSet queryDataSet;
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());

    initContextMap();
    contextMapLocal.get().put(req.queryId, context);

    queryDataSet = processor.getExecutor().processQuery(physicalPlan,
        context);

    queryRet.get().put(statement, queryDataSet);
    return queryDataSet;
  }

  private void initContextMap() {
    Map<Long, QueryContext> contextMap = contextMapLocal.get();
    if (contextMap == null) {
      contextMap = new HashMap<>();
      contextMapLocal.set(contextMap);
    }
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin()) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
      }
      String statement = req.getStatement();
      return executeUpdateStatement(statement);
    } catch (ProcessorException e) {
      LOGGER.error("meet error while executing update statement.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    } catch (Exception e) {
      LOGGER.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan) {
    List<Path> paths = plan.getPaths();

    try {
      if (!checkAuthorization(paths, plan)) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "No permissions for this operation " + plan.getOperatorType());
      }
    } catch (AuthException e) {
      LOGGER.error("meet error while checking authorization.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Uninitialized authorizer " + e.getMessage());
    }
    // TODO
    // In current version, we only return OK/ERROR
    // Do we need to add extra information of executive condition
    boolean execRet;
    try {
      execRet = executeNonQuery(plan);
    } catch (ProcessorException e) {
      LOGGER.debug("meet error while processing non-query. ", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
    TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
    String msg = execRet ? "Execute successfully" : "Execute statement error.";
    TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle;
    operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  private boolean executeNonQuery(PhysicalPlan plan) throws ProcessorException {
    return processor.getExecutor().processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement)
      throws ProcessorException {

    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      physicalPlan.setProposer(username.get());
    } catch (QueryProcessorException | ArgsErrorException e) {
      LOGGER.error("meet error while parsing SQL to physical plan!", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    if (physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Statement is a query statement.");
    }

    return executeUpdateStatement(physicalPlan);
  }

  private void recordANewQuery(String statement, PhysicalPlan physicalPlan) {
    queryStatus.get().put(statement, physicalPlan);
    // refresh current queryRet for statement
    queryRet.get().remove(statement);
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  private boolean checkLogin() {
    return username.get() != null;
  }

  private boolean checkAuthorization(List<Path> paths, PhysicalPlan plan) throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username.get(), paths, plan.getOperatorType(), targetUser);
  }

  private TSExecuteStatementResp getTSExecuteStatementResp(TS_StatusCode code, String msg) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  private TSExecuteBatchStatementResp getTSBathExecuteStatementResp(TS_StatusCode code,
      String msg,
      List<Integer> result) {
    TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    resp.setResult(result);
    return resp;
  }

  private TSFetchResultsResp getTSFetchResultsResp(TS_StatusCode code, String msg) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    return resp;
  }

  void handleClientExit() {
    closeOperation(null);
    closeSession(null);
  }

  @Override
  public TSGetTimeZoneResp getTimeZone() {
    TS_Status tsStatus;
    TSGetTimeZoneResp resp;
    try {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      resp = new TSGetTimeZoneResp(tsStatus, zoneIds.get().toString());
    } catch (Exception e) {
      LOGGER.error("meet error while generating time zone.", e);
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage(e.getMessage());
      resp = new TSGetTimeZoneResp(tsStatus, "Unknown time zone");
    }
    return resp;
  }

  @Override
  public TSSetTimeZoneResp setTimeZone(TSSetTimeZoneReq req) {
    TS_Status tsStatus;
    try {
      String timeZoneID = req.getTimeZone();
      zoneIds.set(ZoneId.of(timeZoneID));
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      LOGGER.error("meet error while setting time zone.", e);
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage(e.getMessage());
    }
    return new TSSetTimeZoneResp(tsStatus);
  }

  @Override
  public ServerProperties getProperties() {
    ServerProperties properties = new ServerProperties();
    properties.setVersion(IoTDBConstant.VERSION);
    properties.setSupportedTimeAggregationOperations(new ArrayList<>());
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MAX_TIME);
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MIN_TIME);
    return properties;
  }

}

