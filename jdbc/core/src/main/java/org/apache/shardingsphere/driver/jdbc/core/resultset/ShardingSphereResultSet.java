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

package org.apache.shardingsphere.driver.jdbc.core.resultset;

import org.apache.shardingsphere.driver.jdbc.adapter.AbstractResultSetAdapter;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.jdbc.type.util.ResultSetUtils;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * ShardingSphere result set.
 */
public final class ShardingSphereResultSet extends AbstractResultSetAdapter {
    
    private static final String ASCII = "Ascii";
    
    private static final String UNICODE = "Unicode";
    
    private static final String BINARY = "Binary";
    
    private final MergedResult mergeResultSet;
    
    private final Map<String, Integer> columnLabelAndIndexMap;
    
    public ShardingSphereResultSet(final List<ResultSet> resultSets, final MergedResult mergeResultSet, final Statement statement, final boolean selectContainsEnhancedTable,
                                   final ExecutionContext executionContext) throws SQLException {
        super(resultSets, statement, selectContainsEnhancedTable, executionContext);
        this.mergeResultSet = mergeResultSet;
        columnLabelAndIndexMap = ShardingSphereResultSetUtils.createColumnLabelAndIndexMap(executionContext.getSqlStatementContext(), selectContainsEnhancedTable, resultSets.get(0).getMetaData());
    }
    
    public ShardingSphereResultSet(final List<ResultSet> resultSets, final MergedResult mergeResultSet, final Statement statement, final boolean selectContainsEnhancedTable,
                                   final ExecutionContext executionContext, final Map<String, Integer> columnLabelAndIndexMap) {
        super(resultSets, statement, selectContainsEnhancedTable, executionContext);
        this.mergeResultSet = mergeResultSet;
        this.columnLabelAndIndexMap = columnLabelAndIndexMap;
    }
    
    @Override
    public boolean next() throws SQLException {
        return mergeResultSet.next();
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        return mergeResultSet.wasNull();
    }
    
    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        return (boolean) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, boolean.class), boolean.class);
    }
    
    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        return getBoolean(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        return (byte) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, byte.class), byte.class);
    }
    
    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        return getByte(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public short getShort(final int columnIndex) throws SQLException {
        return (short) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, short.class), short.class);
    }
    
    @Override
    public short getShort(final String columnLabel) throws SQLException {
        return getShort(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public int getInt(final int columnIndex) throws SQLException {
        return (int) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, int.class), int.class);
    }
    
    @Override
    public int getInt(final String columnLabel) throws SQLException {
        return getInt(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public long getLong(final int columnIndex) throws SQLException {
        return (long) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, long.class), long.class);
    }
    
    @Override
    public long getLong(final String columnLabel) throws SQLException {
        return getLong(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        return (float) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, float.class), float.class);
    }
    
    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        return getFloat(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        return (double) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, double.class), double.class);
    }
    
    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        return getDouble(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public String getString(final int columnIndex) throws SQLException {
        return (String) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, String.class), String.class);
    }
    
    @Override
    public String getString(final String columnLabel) throws SQLException {
        return getString(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public String getNString(final int columnIndex) throws SQLException {
        return getString(columnIndex);
    }
    
    @Override
    public String getNString(final String columnLabel) throws SQLException {
        return getString(columnLabel);
    }
    
    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return (BigDecimal) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, BigDecimal.class), BigDecimal.class);
    }
    
    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        return getBigDecimal(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        return (BigDecimal) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, BigDecimal.class), BigDecimal.class);
    }
    
    @Override
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        return getBigDecimal(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        return (byte[]) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, byte[].class), byte[].class);
    }
    
    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        return getBytes(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        return (Date) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, Date.class), Date.class);
    }
    
    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        return getDate(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        return (Date) ResultSetUtils.convertValue(mergeResultSet.getCalendarValue(columnIndex, Date.class, cal), Date.class);
    }
    
    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        return getDate(getIndexFromColumnLabelAndIndexMap(columnLabel), cal);
    }
    
    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        return (Time) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, Time.class), Time.class);
    }
    
    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        return getTime(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        return (Time) ResultSetUtils.convertValue(mergeResultSet.getCalendarValue(columnIndex, Time.class, cal), Time.class);
    }
    
    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        return getTime(getIndexFromColumnLabelAndIndexMap(columnLabel), cal);
    }
    
    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        return (Timestamp) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, Timestamp.class), Timestamp.class);
    }
    
    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        return getTimestamp(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        return (Timestamp) ResultSetUtils.convertValue(mergeResultSet.getCalendarValue(columnIndex, Timestamp.class, cal), Timestamp.class);
    }
    
    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
        return getTimestamp(getIndexFromColumnLabelAndIndexMap(columnLabel), cal);
    }
    
    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        return mergeResultSet.getInputStream(columnIndex, ASCII);
    }
    
    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        return getAsciiStream(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
        return mergeResultSet.getInputStream(columnIndex, UNICODE);
    }
    
    @Override
    public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
        return getUnicodeStream(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        return mergeResultSet.getInputStream(columnIndex, BINARY);
    }
    
    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        return getBinaryStream(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        // TODO To be supported: encrypt, mask, and so on
        return mergeResultSet.getCharacterStream(columnIndex);
    }
    
    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        return getCharacterStream(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        return (Blob) mergeResultSet.getValue(columnIndex, Blob.class);
    }
    
    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        return getBlob(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        return (Clob) mergeResultSet.getValue(columnIndex, Clob.class);
    }
    
    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        return getClob(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        return (Array) mergeResultSet.getValue(columnIndex, Array.class);
    }
    
    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        return getArray(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        return (URL) mergeResultSet.getValue(columnIndex, URL.class);
    }
    
    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        return getURL(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        return (SQLXML) mergeResultSet.getValue(columnIndex, SQLXML.class);
    }
    
    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        return getSQLXML(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        return mergeResultSet.getValue(columnIndex, Object.class);
    }
    
    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        return getObject(getIndexFromColumnLabelAndIndexMap(columnLabel));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        if (BigInteger.class.equals(type)) {
            return (T) BigInteger.valueOf(getLong(columnIndex));
        }
        if (Blob.class.equals(type)) {
            return (T) getBlob(columnIndex);
        }
        if (Clob.class.equals(type)) {
            return (T) getClob(columnIndex);
        }
        if (LocalDateTime.class.equals(type) || LocalDate.class.equals(type) || LocalTime.class.equals(type) || OffsetDateTime.class.equals(type)) {
            return (T) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, Timestamp.class), type);
        }
        return (T) ResultSetUtils.convertValue(mergeResultSet.getValue(columnIndex, type), type);
    }
    
    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        return getObject(getIndexFromColumnLabelAndIndexMap(columnLabel), type);
    }
    
    private Integer getIndexFromColumnLabelAndIndexMap(final String columnLabel) throws SQLException {
        Integer result = columnLabelAndIndexMap.get(columnLabel);
        ShardingSpherePreconditions.checkState(null != result, () -> new SQLFeatureNotSupportedException(String.format("Can not get index from column label `%s`.", columnLabel)));
        return result;
    }
}
