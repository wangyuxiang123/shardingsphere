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

package org.apache.shardingsphere.test.it.rewrite.engine.scenario;

import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereIndex;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.infra.yaml.config.pojo.YamlRootConfiguration;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.test.it.rewrite.engine.SQLRewriterIT;
import org.apache.shardingsphere.test.it.rewrite.engine.SQLRewriterITSettings;
import org.apache.shardingsphere.test.it.rewrite.engine.parameter.SQLRewriteEngineTestParameters;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@SQLRewriterITSettings("scenario/mix/case")
class MixSQLRewriterIT extends SQLRewriterIT {
    
    @Override
    protected YamlRootConfiguration createRootConfiguration(final SQLRewriteEngineTestParameters testParams) throws IOException {
        URL url = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(testParams.getRuleFile()), "Can not find rewrite rule yaml configurations");
        return YamlEngine.unmarshal(new File(url.getFile()), YamlRootConfiguration.class);
    }
    
    @Override
    protected Map<String, ShardingSphereSchema> mockSchemas(final String schemaName) {
        Map<String, ShardingSphereTable> tables = new LinkedHashMap<>();
        tables.put("t_account", new ShardingSphereTable("t_account", Arrays.asList(
                new ShardingSphereColumn("account_id", Types.INTEGER, true, true, false, true, false),
                new ShardingSphereColumn("password", Types.VARCHAR, false, false, false, true, false),
                new ShardingSphereColumn("amount", Types.DECIMAL, false, false, false, true, false),
                new ShardingSphereColumn("status", Types.TINYINT, false, false, false, false, false)), Collections.singletonList(new ShardingSphereIndex("index_name")), Collections.emptyList()));
        tables.put("t_account_bak", new ShardingSphereTable("t_account_bak", Arrays.asList(
                new ShardingSphereColumn("account_id", Types.INTEGER, true, true, false, true, false),
                new ShardingSphereColumn("password", Types.VARCHAR, false, false, false, true, false),
                new ShardingSphereColumn("amount", Types.DECIMAL, false, false, false, true, false),
                new ShardingSphereColumn("status", Types.TINYINT, false, false, false, false, false)), Collections.emptyList(), Collections.emptyList()));
        tables.put("t_account_detail", new ShardingSphereTable("t_account_detail", Arrays.asList(
                new ShardingSphereColumn("account_id", Types.INTEGER, false, false, false, true, false),
                new ShardingSphereColumn("password", Types.VARCHAR, false, false, false, true, false),
                new ShardingSphereColumn("amount", Types.DECIMAL, false, false, false, true, false),
                new ShardingSphereColumn("status", Types.TINYINT, false, false, false, false, false)), Collections.emptyList(), Collections.emptyList()));
        ShardingSphereSchema result = new ShardingSphereSchema(tables, Collections.emptyMap());
        return Collections.singletonMap(schemaName, result);
    }
    
    @Override
    protected void mockRules(final Collection<ShardingSphereRule> rules, final String schemaName, final SQLStatement sqlStatement) {
    }
    
    @Override
    protected void mockDataSource(final Map<String, DataSource> dataSources) {
    }
}
