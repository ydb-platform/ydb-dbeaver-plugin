/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.ydb.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.Log;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcRequestSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.operation.OperationBinder;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.proto.table.v1.TableServiceGrpc;
import tech.ydb.table.SessionRetryContext;

/**
 * Helper for making raw gRPC DescribeTable calls that bypass the SDK's type check.
 * This allows describing external tables and external data sources at the proto level.
 *
 * <h3>TODO: migrate to dedicated DescribeExternal* RPCs</h3>
 * <p>
 * Currently we use {@code DescribeTable} for external data sources and external tables
 * because the YDB Java SDK ({@code ydb-proto-api}) does not yet include the dedicated
 * proto messages. When a new version of {@code ydb-proto-api} adds them, refactor as follows:
 * </p>
 * <ol>
 *   <li>Check that {@code YdbTable.DescribeExternalDataSourceRequest},
 *       {@code YdbTable.DescribeExternalDataSourceResult},
 *       {@code YdbTable.DescribeExternalTableRequest}, and
 *       {@code YdbTable.DescribeExternalTableResult} exist in the JAR:
 *       <pre>jar tf ydb-jdbc-driver-shaded-X.Y.Z.jar | grep DescribeExternal</pre>
 *   </li>
 *   <li>Add two new methods in this class:
 *       <ul>
 *         <li>{@code describeExternalDataSource(transport, retryCtx, path)} &rarr;
 *             uses {@code TableServiceGrpc.getDescribeExternalDataSourceMethod()},
 *             returns {@code DescribeExternalDataSourceResult}
 *             (fields: {@code source_type}, {@code location}, {@code properties} map)</li>
 *         <li>{@code describeExternalTable(transport, retryCtx, path)} &rarr;
 *             uses {@code TableServiceGrpc.getDescribeExternalTableMethod()},
 *             returns {@code DescribeExternalTableResult}
 *             (fields: {@code source_type}, {@code data_source_path}, {@code location},
 *             {@code columns} list, {@code content} map)</li>
 *       </ul>
 *   </li>
 *   <li>Update {@code YDBExternalDataSource.loadProperties()} to use
 *       {@code describeExternalDataSource()} and read fields directly
 *       ({@code result.getSourceType()}, {@code result.getLocation()}, etc.)
 *       instead of parsing {@code DescribeTableResult.getAttributesMap()}.</li>
 *   <li>Update {@code YDBExternalTable.loadProperties()} to use
 *       {@code describeExternalTable()} and read fields directly.
 *       Columns can be taken from {@code result.getColumnsList()} instead of
 *       the {@code SELECT * LIMIT 0} fallback.</li>
 *   <li>Remove the generic {@code describeTable()} method from this class
 *       (or keep it only for regular tables if needed).</li>
 *   <li>Update {@code plugin.xml} driver version:
 *       {@code <file type="jar" path="maven:/tech.ydb.jdbc:ydb-jdbc-driver-shaded:RELEASE[X.Y.Z]"/>}</li>
 * </ol>
 *
 * <p>Proto definitions (from ydb/public/api/protos/ydb_table.proto):</p>
 * <pre>
 * message DescribeExternalDataSourceResult {
 *     Ydb.Scheme.Entry self = 1;
 *     optional string source_type = 2;
 *     optional string location = 3;
 *     map&lt;string, string&gt; properties = 4;
 * }
 *
 * message DescribeExternalTableResult {
 *     Ydb.Scheme.Entry self = 1;
 *     optional string source_type = 2;
 *     optional string data_source_path = 3;
 *     optional string location = 4;
 *     repeated ColumnMeta columns = 5;
 *     map&lt;string, string&gt; content = 6;
 * }
 * </pre>
 */
final class YDBDescribeHelper {

    private static final Log log = Log.getLog(YDBDescribeHelper.class);

    private YDBDescribeHelper() {
    }

    @Nullable
    static YdbTable.DescribeTableResult describeTable(
        @NotNull GrpcTransport transport,
        @NotNull SessionRetryContext retryCtx,
        @NotNull String path
    ) {
        try {
            Result<YdbTable.DescribeTableResult> result = retryCtx.<YdbTable.DescribeTableResult>supplyResult(
                session -> {
                    YdbTable.DescribeTableRequest request = YdbTable.DescribeTableRequest.newBuilder()
                        .setSessionId(session.getId())
                        .setPath(path)
                        .build();

                    GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();

                    return transport
                        .unaryCall(TableServiceGrpc.getDescribeTableMethod(), settings, request)
                        .thenApply(OperationBinder.bindSync(
                            YdbTable.DescribeTableResponse::getOperation,
                            YdbTable.DescribeTableResult.class
                        ));
                }
            ).join();

            if (result.isSuccess()) {
                return result.getValue();
            } else {
                log.debug("DescribeTable failed for " + path + ": " + result.getStatus());
                return null;
            }
        } catch (Exception e) {
            log.debug("DescribeTable error for " + path + ": " + e.getMessage());
            return null;
        }
    }
}
