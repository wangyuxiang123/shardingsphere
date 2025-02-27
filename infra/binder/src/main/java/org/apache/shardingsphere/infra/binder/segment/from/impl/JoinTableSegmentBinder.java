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

package org.apache.shardingsphere.infra.binder.segment.from.impl;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.binder.segment.from.TableSegmentBinder;
import org.apache.shardingsphere.infra.binder.segment.from.TableSegmentBinderContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.JoinTableSegment;

import java.util.Map;

/**
 * Join table segment binder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JoinTableSegmentBinder {
    
    /**
     * Bind join table segment with metadata.
     *
     * @param segment join table segment
     * @param metaData meta data
     * @param defaultDatabaseName default database name
     * @param databaseType database type
     * @param tableBinderContexts table binder contexts
     * @return bounded join table segment
     */
    public static JoinTableSegment bind(final JoinTableSegment segment, final ShardingSphereMetaData metaData, final String defaultDatabaseName, final DatabaseType databaseType,
                                        final Map<String, TableSegmentBinderContext> tableBinderContexts) {
        segment.setLeft(TableSegmentBinder.bind(segment.getLeft(), metaData, defaultDatabaseName, databaseType, tableBinderContexts));
        segment.setRight(TableSegmentBinder.bind(segment.getRight(), metaData, defaultDatabaseName, databaseType, tableBinderContexts));
        // TODO bind condition and using column in join table segment
        return segment;
    }
}
