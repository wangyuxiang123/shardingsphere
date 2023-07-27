/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.test.e2e.engine.type;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.test.e2e.cases.SQLCommandType;
import org.apache.shardingsphere.test.e2e.cases.assertion.IntegrationTestCaseAssertion;
import org.apache.shardingsphere.test.e2e.cases.dataset.metadata.DataSetColumn;
import org.apache.shardingsphere.test.e2e.cases.dataset.metadata.DataSetMetaData;
import org.apache.shardingsphere.test.e2e.cases.dataset.row.DataSetRow;
import org.apache.shardingsphere.test.e2e.engine.arg.E2ETestCaseArgumentsProvider;
import org.apache.shardingsphere.test.e2e.engine.arg.E2ETestCaseSettings;
import org.apache.shardingsphere.test.e2e.engine.composer.SingleE2EContainerComposer;
import org.apache.shardingsphere.test.e2e.framework.param.array.E2ETestParameterFactory;
import org.apache.shardingsphere.test.e2e.framework.param.model.AssertionTestParameter;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

// 资源和规划的创建、修改、删除：
// 管理逻辑库下的存储单元
// 分片、加密、读写分离、脱敏
// 2023.7.18 写差不多了
@Slf4j
@E2ETestCaseSettings(SQLCommandType.RDL)
class RDLE2EIT {

    @ParameterizedTest(name = "{0}")
    @EnabledIf("isEnabled")
    @ArgumentsSource(E2ETestCaseArgumentsProvider.class)
    void assertExecute(final AssertionTestParameter testParam) throws SQLException {
        // TODO make sure test case can not be null
        if (null == testParam.getTestCaseContext()) {
            return;
        }
        SingleE2EContainerComposer containerComposer = new SingleE2EContainerComposer(testParam);

        if (null == containerComposer.getAssertions()) {
            return;
        }
        int dataSetIndex = 0;
        String mode = testParam.getMode();
        log.info("testParam.getMode:" + mode);
        for (IntegrationTestCaseAssertion each : containerComposer.getAssertions()) {
            log.info("dataSetIndex" + dataSetIndex);
            if (each.getInitialSQL() != null && each.getAssertionSQL() != null) {
                log.info("-----------------start init-----------------");
                log.info("getInitialSQL: " + each.getInitialSQL().getSql());
                init(containerComposer, each, dataSetIndex, mode);
                log.info("-----------------end init-----------------");
            }
            if (each.getAssertionSQL() != null && each.getDestroySQL() == null && each.getInitialSQL() == null) {
                log.info("-----------------start executeSQLCase-----------------");
                log.info("getAssertionSQL getSql: " + each.getAssertionSQL().getSql());
                executeSQLCase(containerComposer, each, dataSetIndex, mode);
                log.info("-----------------end executeSQLCase-----------------");
            }
            if (each.getDestroySQL() != null && each.getAssertionSQL() != null) {
                log.info("-----------------start tearDown-----------------");
                log.info("getDestroySQL: " + each.getDestroySQL().getSql());
                tearDown(containerComposer, each, dataSetIndex, mode);
                log.info("-----------------end tearDown-----------------");
            }
            dataSetIndex++;
        }
    }

    private void executeSQLCase(final SingleE2EContainerComposer containerComposer, IntegrationTestCaseAssertion testCaseAssertionExecuteSql, int dataSetIndex, String mode) throws SQLException {
        try (Connection connection = containerComposer.getTargetDataSource().getConnection()) {
            try (Statement statement = connection.createStatement()) {
                log.info("executeSQLCase:" + containerComposer.getSQL());
                statement.execute(containerComposer.getSQL());
                log.info("executeSQLCase 执行成功");

                if (mode.equals("Cluster")) {
                    Awaitility.await().atMost(Durations.ONE_MINUTE).until(() -> assertResultSet(containerComposer, statement, testCaseAssertionExecuteSql, dataSetIndex));
                } else if (mode.equals("Standalone")) {
                    assertResultSet(containerComposer, statement, testCaseAssertionExecuteSql, dataSetIndex);
                }
            } catch (Exception e) {
                log.info("executeSQLCase Exception:" + e);
            }
        }
    }

    private void init(final SingleE2EContainerComposer containerComposer, IntegrationTestCaseAssertion testCaseAssertionInitSql, int dataSetIndex, String mode) throws SQLException {
        try (Connection connection = containerComposer.getTargetDataSource().getConnection()) {
            executeInitSQLs(containerComposer, connection, testCaseAssertionInitSql, dataSetIndex, mode);
        }
    }

    private void tearDown(final SingleE2EContainerComposer containerComposer, IntegrationTestCaseAssertion testCaseAssertionDestroySQL, int dataSetIndex, String mode) throws SQLException {
        try (Connection connection = containerComposer.getTargetDataSource().getConnection()) {
            executeDestroySQLs(containerComposer, connection, testCaseAssertionDestroySQL, dataSetIndex, mode);
        }
    }

    private void executeInitSQLs(final SingleE2EContainerComposer containerComposer, final Connection connection, IntegrationTestCaseAssertion testCaseAssertionInitSql,
                                 int dataSetIndex, String mode) throws SQLException {
        if (null == testCaseAssertionInitSql.getInitialSQL() || null == testCaseAssertionInitSql.getInitialSQL().getSql()) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(testCaseAssertionInitSql.getInitialSQL().getSql());
            log.info("executeInitSQLs 执行成功");
            if (mode.equals("Cluster")) {
                Awaitility.await().atMost(Durations.ONE_MINUTE).until(() -> assertResultSet(containerComposer, statement, testCaseAssertionInitSql, dataSetIndex));
            } else if (mode.equals("Standalone")) {
                assertResultSet(containerComposer, statement, testCaseAssertionInitSql, dataSetIndex);
            }
        } catch (Exception e) {
            log.info("executeInitSQLs Exception:" + e);
        }
    }

    private void executeDestroySQLs(final SingleE2EContainerComposer containerComposer, final Connection connection, IntegrationTestCaseAssertion testCaseAssertionDestroySQL,
                                    int dataSetIndex, String mode) throws SQLException {
        if (null == testCaseAssertionDestroySQL.getDestroySQL() || null == testCaseAssertionDestroySQL.getDestroySQL().getSql()) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(testCaseAssertionDestroySQL.getDestroySQL().getSql());
            log.info("executeDestroySQLs 执行成功");
            if (mode.equals("Cluster")) {
                Awaitility.await().atMost(Durations.ONE_MINUTE).until(() -> assertResultSet(containerComposer, statement, testCaseAssertionDestroySQL, dataSetIndex));
            } else if (mode.equals("Standalone")) {
                assertResultSet(containerComposer, statement, testCaseAssertionDestroySQL, dataSetIndex);
            }
        } catch (Exception e) {
            log.info("executeDestroySQLs Exception:" + e);
        }
    }

    private boolean assertResultSet(final SingleE2EContainerComposer containerComposer, final Statement statement, final IntegrationTestCaseAssertion testCaseAssertionAssertionSql,
                                    int dataSetIndex) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(testCaseAssertionAssertionSql.getAssertionSQL().getSql())) {
            log.info("assertResultSet getAssertionSQL:" + testCaseAssertionAssertionSql.getAssertionSQL().getSql());
            assertResultSet(containerComposer, resultSet, dataSetIndex);
            log.info("----------------断言成功----------");
            return true;
        } catch (Exception e) {
            log.info("断言失败，问题是:" + e);
            return false;
        }
    }

    private void assertResultSet(final SingleE2EContainerComposer containerComposer, final ResultSet resultSet, int dataSetIndex) throws SQLException {
        assertMetaData(resultSet.getMetaData(), getExpectedColumns(containerComposer, dataSetIndex));
        assertRows(resultSet, containerComposer.getDataSets().get(dataSetIndex).getRows());
    }

    private Collection<DataSetColumn> getExpectedColumns(final SingleE2EContainerComposer containerComposer, int dataSetIndex) {
        Collection<DataSetColumn> result = new LinkedList<>();
        List<DataSetMetaData> dataSetMetaDataList = containerComposer.getDataSets().get(dataSetIndex).getMetaDataList();
        for (DataSetMetaData each : dataSetMetaDataList) {
            result.addAll(each.getColumns());
        }
        return result;
    }

    private void assertMetaData(final ResultSetMetaData actual, final Collection<DataSetColumn> expected) throws SQLException {
        assertThat(actual.getColumnCount(), is(expected.size()));
        int index = 1;
        for (DataSetColumn each : expected) {
            assertThat(actual.getColumnLabel(index++).toLowerCase(), is(each.getName().toLowerCase()));
        }
    }

    private void assertRows(final ResultSet actual, final List<DataSetRow> expected) throws SQLException {
        int rowCount = 0;
        ResultSetMetaData actualMetaData = actual.getMetaData();
        while (actual.next()) {
            assertTrue(rowCount < expected.size(), "Size of actual result set is different with size of expected dat set rows.");
            assertRow(actual, actualMetaData, expected.get(rowCount));
            rowCount++;
        }
        assertThat("Size of actual result set is different with size of expected dat set rows.", rowCount, is(expected.size()));
    }

    private void assertRow(final ResultSet actual, final ResultSetMetaData actualMetaData, final DataSetRow expected) throws SQLException {
        int columnIndex = 1;
        for (String each : expected.splitValues("|")) {
            String columnLabel = actualMetaData.getColumnLabel(columnIndex);
            assertObjectValue(actual, columnIndex, columnLabel, each);
            columnIndex++;
        }
    }

    private void assertObjectValue(final ResultSet actual, final int columnIndex, final String columnLabel, final String expected) throws SQLException {
        assertThat(String.valueOf(actual.getObject(columnIndex)), is(expected));
        assertThat(String.valueOf(actual.getObject(columnLabel)), is(expected));
    }

    private static boolean isEnabled() {
        return E2ETestParameterFactory.containsTestParameter() && !E2ETestParameterFactory.getAssertionTestParameters(SQLCommandType.RDL).isEmpty();
    }
}
