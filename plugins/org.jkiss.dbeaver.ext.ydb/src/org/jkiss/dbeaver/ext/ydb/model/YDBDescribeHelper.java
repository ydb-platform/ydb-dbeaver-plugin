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
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.proto.scheme.v1.SchemeServiceGrpc;
import tech.ydb.proto.table.v1.TableServiceGrpc;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.table.SessionRetryContext;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

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

    /**
     * Check if the proto type is OPTIONAL (nullable wrapper).
     */
    static boolean isOptionalType(@NotNull ValueProtos.Type type) {
        return type.getTypeCase() == ValueProtos.Type.TypeCase.OPTIONAL_TYPE;
    }

    /**
     * Unwrap OPTIONAL type to get the inner type.
     * Returns the type itself if not OPTIONAL.
     */
    @NotNull
    static ValueProtos.Type unwrapOptionalType(@NotNull ValueProtos.Type type) {
        if (type.getTypeCase() == ValueProtos.Type.TypeCase.OPTIONAL_TYPE) {
            return type.getOptionalType().getItem();
        }
        return type;
    }

    /**
     * Determine if a column is nullable based on its proto type and not_null flag.
     * A column is NOT NULL if:
     * - The ColumnMeta has not_null = true, OR
     * - The type is NOT wrapped in OPTIONAL
     */
    static boolean isNotNull(@NotNull YdbTable.ColumnMeta column) {
        try {
            if (column.hasNotNull() && column.getNotNull()) {
                return true;
            }
        } catch (NoSuchMethodError ignored) {
            // older proto-api may not have not_null field
        }
        return !isOptionalType(column.getType());
    }

    /**
     * Resolve a human-readable type name from a proto Type.
     */
    @NotNull
    static String resolveTypeName(@NotNull ValueProtos.Type type) {
        switch (type.getTypeCase()) {
            case TYPE_ID:
                return resolvePrimitiveTypeName(type.getTypeId());
            case OPTIONAL_TYPE:
                return resolveTypeName(type.getOptionalType().getItem());
            case DECIMAL_TYPE:
                return "Decimal(" + type.getDecimalType().getPrecision()
                    + "," + type.getDecimalType().getScale() + ")";
            case LIST_TYPE:
                return "List<" + resolveTypeName(type.getListType().getItem()) + ">";
            case STRUCT_TYPE:
                return "Struct";
            case TUPLE_TYPE:
                return "Tuple";
            case DICT_TYPE:
                return "Dict";
            case VARIANT_TYPE:
                return "Variant";
            default:
                return type.getTypeCase().name();
        }
    }

    /**
     * Map a proto Type to a JDBC {@link java.sql.Types} constant.
     * Mirrors the mapping logic from the YDB JDBC driver's YdbTypes.toSqlType().
     */
    static int resolveJdbcType(@NotNull ValueProtos.Type type) {
        switch (type.getTypeCase()) {
            case TYPE_ID:
                return resolvePrimitiveJdbcType(type.getTypeId());
            case OPTIONAL_TYPE:
                return resolveJdbcType(type.getOptionalType().getItem());
            case DECIMAL_TYPE:
                return Types.DECIMAL;
            case LIST_TYPE:
                return Types.ARRAY;
            case STRUCT_TYPE:
                return Types.STRUCT;
            case NULL_TYPE:
            case VOID_TYPE:
                return Types.NULL;
            default:
                return Types.OTHER;
        }
    }

    /**
     * Get SQL precision for a proto Type.
     */
    static int getSqlPrecision(@NotNull ValueProtos.Type type) {
        switch (type.getTypeCase()) {
            case TYPE_ID:
                return getPrimitivePrecision(type.getTypeId());
            case OPTIONAL_TYPE:
                return getSqlPrecision(type.getOptionalType().getItem());
            case DECIMAL_TYPE:
                return type.getDecimalType().getPrecision();
            default:
                return 0;
        }
    }

    /**
     * Get scale for a proto Type (relevant for DECIMAL).
     */
    static int getSqlScale(@NotNull ValueProtos.Type type) {
        switch (type.getTypeCase()) {
            case OPTIONAL_TYPE:
                return getSqlScale(type.getOptionalType().getItem());
            case DECIMAL_TYPE:
                return type.getDecimalType().getScale();
            default:
                return 0;
        }
    }

    @NotNull
    private static String resolvePrimitiveTypeName(@NotNull ValueProtos.Type.PrimitiveTypeId typeId) {
        switch (typeId) {
            case BOOL:           return "Bool";
            case INT8:           return "Int8";
            case UINT8:          return "Uint8";
            case INT16:          return "Int16";
            case UINT16:         return "Uint16";
            case INT32:          return "Int32";
            case UINT32:         return "Uint32";
            case INT64:          return "Int64";
            case UINT64:         return "Uint64";
            case FLOAT:          return "Float";
            case DOUBLE:         return "Double";
            case DATE:           return "Date";
            case DATETIME:       return "Datetime";
            case TIMESTAMP:      return "Timestamp";
            case INTERVAL:       return "Interval";
            case TZ_DATE:        return "TzDate";
            case TZ_DATETIME:    return "TzDatetime";
            case TZ_TIMESTAMP:   return "TzTimestamp";
            case STRING:         return "String";
            case UTF8:           return "Utf8";
            case YSON:           return "Yson";
            case JSON:           return "Json";
            case UUID:           return "Uuid";
            case JSON_DOCUMENT:  return "JsonDocument";
            case DYNUMBER:       return "DyNumber";
            default:             return typeId.name();
        }
    }

    private static int resolvePrimitiveJdbcType(@NotNull ValueProtos.Type.PrimitiveTypeId typeId) {
        switch (typeId) {
            case BOOL:
                return Types.BOOLEAN;
            case INT8:
            case INT16:
                return Types.SMALLINT;
            case UINT8:
            case INT32:
            case UINT16:
                return Types.INTEGER;
            case UINT32:
            case INT64:
            case UINT64:
            case INTERVAL:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DATE:
                return Types.DATE;
            case DATETIME:
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case TZ_DATE:
            case TZ_DATETIME:
            case TZ_TIMESTAMP:
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case UTF8:
            case JSON:
            case JSON_DOCUMENT:
            case UUID:
                return Types.VARCHAR;
            case STRING:
            case YSON:
                return Types.BINARY;
            case DYNUMBER:
                return Types.VARCHAR;
            default:
                return Types.OTHER;
        }
    }

    /**
     * Call SchemeClient.describePath() and return the raw proto Entry,
     * which contains owner, permissions, and effective_permissions.
     */
    @Nullable
    static SchemeOperationProtos.Entry describePath(
        @NotNull SchemeClient schemeClient,
        @NotNull String path
    ) {
        try {
            Result<DescribePathResult> result = schemeClient.describePath(path).join();
            if (result.isSuccess()) {
                return result.getValue().getSelf();
            } else {
                log.debug("DescribePath failed for " + path + ": " + result.getStatus());
                return null;
            }
        } catch (Exception e) {
            log.debug("DescribePath error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Call SchemeService.ModifyPermissions RPC directly via GrpcTransport.
     * The Java SDK's SchemeClient does not expose this method,
     * so we call the gRPC method directly.
     *
     * @return true if the operation succeeded
     */
    static boolean modifyPermissions(
        @NotNull GrpcTransport transport,
        @NotNull String path,
        @NotNull List<YDBPermissionHolder.PermissionAction> actions,
        boolean clearPermissions,
        @Nullable Boolean interruptInheritance
    ) {
        try {
            List<SchemeOperationProtos.PermissionsAction> protoActions = new java.util.ArrayList<>();
            for (YDBPermissionHolder.PermissionAction action : actions) {
                SchemeOperationProtos.PermissionsAction.Builder ab =
                    SchemeOperationProtos.PermissionsAction.newBuilder();
                switch (action.getType()) {
                    case GRANT:
                        ab.setGrant(SchemeOperationProtos.Permissions.newBuilder()
                            .setSubject(action.getSubject())
                            .addAllPermissionNames(action.getPermissionNames())
                            .build());
                        break;
                    case REVOKE:
                        ab.setRevoke(SchemeOperationProtos.Permissions.newBuilder()
                            .setSubject(action.getSubject())
                            .addAllPermissionNames(action.getPermissionNames())
                            .build());
                        break;
                    case SET:
                        ab.setSet(SchemeOperationProtos.Permissions.newBuilder()
                            .setSubject(action.getSubject())
                            .addAllPermissionNames(action.getPermissionNames())
                            .build());
                        break;
                    case CHANGE_OWNER:
                        ab.setChangeOwner(action.getNewOwner());
                        break;
                }
                protoActions.add(ab.build());
            }

            SchemeOperationProtos.ModifyPermissionsRequest.Builder builder =
                SchemeOperationProtos.ModifyPermissionsRequest.newBuilder()
                    .setPath(path)
                    .addAllActions(protoActions)
                    .setClearPermissions(clearPermissions);
            if (interruptInheritance != null) {
                builder.setInterruptInheritance(interruptInheritance);
            }
            GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();
            tech.ydb.core.Status status = transport
                .unaryCall(SchemeServiceGrpc.getModifyPermissionsMethod(), settings, builder.build())
                .thenApply(OperationBinder.bindSync(
                    SchemeOperationProtos.ModifyPermissionsResponse::getOperation))
                .join();
            if (status.isSuccess()) {
                return true;
            } else {
                log.error("ModifyPermissions failed for " + path + ": " + status);
                return false;
            }
        } catch (Exception e) {
            log.error("ModifyPermissions error for " + path + ": " + e.getMessage(), e);
            return false;
        }
    }

    /**
     * Format a list of Permissions proto messages into a human-readable string.
     * Each line: "subject: perm1, perm2, ..."
     */
    @NotNull
    static List<YDBPermissionHolder.PermissionEntry> toPermissionEntries(
        @NotNull List<SchemeOperationProtos.Permissions> permissionsList
    ) {
        List<YDBPermissionHolder.PermissionEntry> result = new java.util.ArrayList<>();
        for (SchemeOperationProtos.Permissions perm : permissionsList) {
            result.add(new YDBPermissionHolder.PermissionEntry(
                perm.getSubject(),
                new java.util.ArrayList<>(perm.getPermissionNamesList())
            ));
        }
        return result;
    }

    @NotNull
    static String formatPermissions(@NotNull List<SchemeOperationProtos.Permissions> permissionsList) {
        if (permissionsList.isEmpty()) {
            return "";
        }
        StringJoiner lines = new StringJoiner("\n");
        for (SchemeOperationProtos.Permissions perm : permissionsList) {
            String subject = perm.getSubject();
            List<String> names = perm.getPermissionNamesList();
            if (!names.isEmpty()) {
                lines.add(subject + ": " + String.join(", ", names));
            } else {
                lines.add(subject);
            }
        }
        return lines.toString();
    }

    /**
     * Describe a View using the raw gRPC ViewService/DescribeView RPC.
     * The proto classes for ViewService are not available in the current JAR,
     * so we encode/decode the request/response as raw bytes.
     *
     * <p>Request proto (Ydb.View.DescribeViewRequest):</p>
     * <pre>
     * message DescribeViewRequest {
     *     Ydb.Operations.OperationParams operation_params = 1;
     *     string path = 2;
     * }
     * </pre>
     *
     * <p>Response proto (Ydb.View.DescribeViewResponse):</p>
     * <pre>
     * message DescribeViewResponse {
     *     Ydb.Operations.Operation operation = 1;
     * }
     * </pre>
     *
     * <p>Result proto (Ydb.View.DescribeViewResult):</p>
     * <pre>
     * message DescribeViewResult {
     *     Ydb.Scheme.Entry self = 1;
     *     string query_text = 2;
     * }
     * </pre>
     *
     * @return the query text of the view, or null if the RPC fails
     */
    @Nullable
    static String describeViewQueryText(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        try {
            tech.ydb.shaded.grpc.MethodDescriptor<byte[], byte[]> method = tech.ydb.shaded.grpc.MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(tech.ydb.shaded.grpc.MethodDescriptor.MethodType.UNARY)
                .setFullMethodName("Ydb.View.V1.ViewService/DescribeView")
                .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
                .setResponseMarshaller(ByteArrayMarshaller.INSTANCE)
                .build();

            // Encode DescribeViewRequest: field 2 (path) as length-delimited string
            byte[] pathBytes = path.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] request = new byte[1 + computeVarIntSize(pathBytes.length) + pathBytes.length];
            int offset = 0;
            request[offset++] = (byte) ((2 << 3) | 2); // field 2, wire type 2 (length-delimited)
            offset = writeVarInt(request, offset, pathBytes.length);
            System.arraycopy(pathBytes, 0, request, offset, pathBytes.length);

            GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();
            byte[] responseBytes = transport
                .unaryCall(method, settings, request)
                .join()
                .getValue();

            // Decode DescribeViewResponse: field 1 is Operation (length-delimited)
            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.debug("DescribeView: no operation in response for " + path);
                return null;
            }

            // Check operation status: field 3 (StatusIds.StatusCode) is varint
            // Note: field 2 is "ready" (bool), field 3 is "status"
            // StatusCode: SUCCESS = 400000, STATUS_CODE_UNSPECIFIED = 0
            Integer statusValue = readVarIntField(operationBytes, 3);
            if (statusValue != null && statusValue != 400000 && statusValue != 0) {
                log.debug("DescribeView: operation failed for " + path + ", status=" + statusValue);
                return null;
            }

            // Extract result from operation.result (field 5, google.protobuf.Any)
            byte[] anyBytes = readField(operationBytes, 5);
            if (anyBytes == null) {
                log.debug("DescribeView: no result in operation for " + path);
                return null;
            }

            // google.protobuf.Any: field 2 = value (the serialized DescribeViewResult)
            byte[] resultBytes = readField(anyBytes, 2);
            if (resultBytes == null) {
                log.debug("DescribeView: no value in Any for " + path);
                return null;
            }

            // DescribeViewResult: field 2 = query_text (string)
            byte[] queryTextBytes = readField(resultBytes, 2);
            if (queryTextBytes == null) {
                return "";
            }
            return new String(queryTextBytes, java.nio.charset.StandardCharsets.UTF_8);

        } catch (Exception e) {
            log.debug("DescribeView error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * DTO holding the result of DescribeExternalDataSource RPC.
     */
    static final class ExternalDataSourceInfo {
        final String sourceType;
        final String location;
        final Map<String, String> properties;
        final String owner;
        final List<YDBPermissionHolder.PermissionEntry> permissions;
        final List<YDBPermissionHolder.PermissionEntry> effectivePermissions;

        ExternalDataSourceInfo(
            @Nullable String sourceType,
            @Nullable String location,
            @NotNull Map<String, String> properties,
            @Nullable String owner,
            @NotNull List<YDBPermissionHolder.PermissionEntry> permissions,
            @NotNull List<YDBPermissionHolder.PermissionEntry> effectivePermissions
        ) {
            this.sourceType = sourceType;
            this.location = location;
            this.properties = properties;
            this.owner = owner;
            this.permissions = permissions;
            this.effectivePermissions = effectivePermissions;
        }
    }

    /**
     * Describe an External Data Source using raw gRPC TableService/DescribeExternalDataSource RPC.
     * The proto classes for this RPC are not available in the current shaded JAR,
     * so we encode/decode the request/response as raw bytes.
     *
     * <p>Request proto:</p>
     * <pre>
     * message DescribeExternalDataSourceRequest {
     *     Ydb.Operations.OperationParams operation_params = 1;
     *     string path = 2;
     * }
     * </pre>
     *
     * <p>Result proto:</p>
     * <pre>
     * message DescribeExternalDataSourceResult {
     *     Ydb.Scheme.Entry self = 1;
     *     optional string source_type = 2;
     *     optional string location = 3;
     *     map&lt;string, string&gt; properties = 4;
     * }
     * </pre>
     */
    @Nullable
    static ExternalDataSourceInfo describeExternalDataSource(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        try {
            tech.ydb.shaded.grpc.MethodDescriptor<byte[], byte[]> method =
                tech.ydb.shaded.grpc.MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(tech.ydb.shaded.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("Ydb.Table.V1.TableService/DescribeExternalDataSource")
                    .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
                    .setResponseMarshaller(ByteArrayMarshaller.INSTANCE)
                    .build();

            // Encode DescribeExternalDataSourceRequest: field 2 (path) as length-delimited string
            byte[] pathBytes = path.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] request = new byte[1 + computeVarIntSize(pathBytes.length) + pathBytes.length];
            int offset = 0;
            request[offset++] = (byte) ((2 << 3) | 2); // field 2, wire type 2 (length-delimited)
            offset = writeVarInt(request, offset, pathBytes.length);
            System.arraycopy(pathBytes, 0, request, offset, pathBytes.length);

            GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();
            byte[] responseBytes = transport
                .unaryCall(method, settings, request)
                .join()
                .getValue();

            // Decode response: field 1 is Operation (length-delimited)
            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.debug("DescribeExternalDataSource: no operation in response for " + path);
                return null;
            }

            // Check operation status: field 3 (StatusIds.StatusCode) is varint
            Integer statusValue = readVarIntField(operationBytes, 3);
            if (statusValue != null && statusValue != 400000 && statusValue != 0) {
                log.debug("DescribeExternalDataSource: operation failed for " + path + ", status=" + statusValue);
                return null;
            }

            // Extract result from operation.result (field 5, google.protobuf.Any)
            byte[] anyBytes = readField(operationBytes, 5);
            if (anyBytes == null) {
                log.debug("DescribeExternalDataSource: no result in operation for " + path);
                return null;
            }

            // google.protobuf.Any: field 2 = value (the serialized DescribeExternalDataSourceResult)
            byte[] resultBytes = readField(anyBytes, 2);
            if (resultBytes == null) {
                log.debug("DescribeExternalDataSource: no value in Any for " + path);
                return null;
            }

            // Parse DescribeExternalDataSourceResult fields
            String sourceType = readStringField(resultBytes, 2);
            String location = readStringField(resultBytes, 3);
            Map<String, String> properties = readMapEntries(resultBytes, 4);

            // Parse self (field 1) for owner/permissions
            String owner = null;
            List<YDBPermissionHolder.PermissionEntry> permissions = List.of();
            List<YDBPermissionHolder.PermissionEntry> effectivePermissions = List.of();

            byte[] selfBytes = readField(resultBytes, 1);
            if (selfBytes != null) {
                // Ydb.Scheme.Entry: field 2 = owner, field 7 = permissions, field 6 = effective_permissions
                owner = readStringField(selfBytes, 2);
                permissions = parsePermissionEntries(selfBytes, 7);
                effectivePermissions = parsePermissionEntries(selfBytes, 6);
            }

            return new ExternalDataSourceInfo(sourceType, location, properties, owner, permissions, effectivePermissions);

        } catch (Exception e) {
            log.debug("DescribeExternalDataSource error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * DTO holding the result of DescribeExternalTable RPC.
     */
    static final class ExternalTableInfo {
        final String sourceType;
        final String dataSourcePath;
        final String location;
        final List<YdbTable.ColumnMeta> columns;
        final Map<String, String> content;
        final String owner;
        final List<YDBPermissionHolder.PermissionEntry> permissions;
        final List<YDBPermissionHolder.PermissionEntry> effectivePermissions;

        ExternalTableInfo(
            @Nullable String sourceType,
            @Nullable String dataSourcePath,
            @Nullable String location,
            @NotNull List<YdbTable.ColumnMeta> columns,
            @NotNull Map<String, String> content,
            @Nullable String owner,
            @NotNull List<YDBPermissionHolder.PermissionEntry> permissions,
            @NotNull List<YDBPermissionHolder.PermissionEntry> effectivePermissions
        ) {
            this.sourceType = sourceType;
            this.dataSourcePath = dataSourcePath;
            this.location = location;
            this.columns = columns;
            this.content = content;
            this.owner = owner;
            this.permissions = permissions;
            this.effectivePermissions = effectivePermissions;
        }
    }

    /**
     * Describe an External Table using raw gRPC TableService/DescribeExternalTable RPC.
     * The proto classes for this RPC are not available in the current shaded JAR,
     * so we encode/decode the request/response as raw bytes.
     * Columns (field 5, repeated ColumnMeta) are parsed back via {@code YdbTable.ColumnMeta.parseFrom()}.
     *
     * <p>Result proto:</p>
     * <pre>
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
    @Nullable
    static ExternalTableInfo describeExternalTable(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        try {
            tech.ydb.shaded.grpc.MethodDescriptor<byte[], byte[]> method =
                tech.ydb.shaded.grpc.MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(tech.ydb.shaded.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("Ydb.Table.V1.TableService/DescribeExternalTable")
                    .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
                    .setResponseMarshaller(ByteArrayMarshaller.INSTANCE)
                    .build();

            // Encode DescribeExternalTableRequest: field 2 (path) as length-delimited string
            byte[] pathBytes = path.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] request = new byte[1 + computeVarIntSize(pathBytes.length) + pathBytes.length];
            int offset = 0;
            request[offset++] = (byte) ((2 << 3) | 2); // field 2, wire type 2 (length-delimited)
            offset = writeVarInt(request, offset, pathBytes.length);
            System.arraycopy(pathBytes, 0, request, offset, pathBytes.length);

            GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();
            byte[] responseBytes = transport
                .unaryCall(method, settings, request)
                .join()
                .getValue();

            // Decode response: field 1 is Operation (length-delimited)
            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.debug("DescribeExternalTable: no operation in response for " + path);
                return null;
            }

            // Check operation status: field 3 (StatusIds.StatusCode) is varint
            Integer statusValue = readVarIntField(operationBytes, 3);
            if (statusValue != null && statusValue != 400000 && statusValue != 0) {
                log.debug("DescribeExternalTable: operation failed for " + path + ", status=" + statusValue);
                return null;
            }

            // Extract result from operation.result (field 5, google.protobuf.Any)
            byte[] anyBytes = readField(operationBytes, 5);
            if (anyBytes == null) {
                log.debug("DescribeExternalTable: no result in operation for " + path);
                return null;
            }

            // google.protobuf.Any: field 2 = value (the serialized DescribeExternalTableResult)
            byte[] resultBytes = readField(anyBytes, 2);
            if (resultBytes == null) {
                log.debug("DescribeExternalTable: no value in Any for " + path);
                return null;
            }

            // Parse DescribeExternalTableResult fields
            String sourceType = readStringField(resultBytes, 2);
            String dataSourcePath = readStringField(resultBytes, 3);
            String location = readStringField(resultBytes, 4);
            Map<String, String> content = readMapEntries(resultBytes, 6);

            // Parse columns (field 5, repeated ColumnMeta) — each entry is raw bytes
            // that can be parsed via the proto ColumnMeta class
            List<byte[]> columnRawList = readAllFields(resultBytes, 5);
            List<YdbTable.ColumnMeta> columns = new ArrayList<>();
            for (byte[] colBytes : columnRawList) {
                try {
                    columns.add(YdbTable.ColumnMeta.parseFrom(colBytes));
                } catch (tech.ydb.shaded.google.protobuf.InvalidProtocolBufferException e) {
                    log.debug("Failed to parse ColumnMeta for " + path + ": " + e.getMessage());
                }
            }

            // Parse self (field 1) for owner/permissions
            String owner = null;
            List<YDBPermissionHolder.PermissionEntry> permissions = List.of();
            List<YDBPermissionHolder.PermissionEntry> effectivePermissions = List.of();

            byte[] selfBytes = readField(resultBytes, 1);
            if (selfBytes != null) {
                owner = readStringField(selfBytes, 2);
                permissions = parsePermissionEntries(selfBytes, 7);
                effectivePermissions = parsePermissionEntries(selfBytes, 6);
            }

            return new ExternalTableInfo(
                sourceType, dataSourcePath, location, columns, content,
                owner, permissions, effectivePermissions);

        } catch (Exception e) {
            log.debug("DescribeExternalTable error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * DTO holding the result of DescribeTransfer RPC.
     */
    static final class TransferInfo {
        final String sourcePath;
        final String destinationPath;
        final String sourceConnection;
        final String transformationLambda;
        final String state;
        final String consumerName;
        final String owner;
        final List<YDBPermissionHolder.PermissionEntry> permissions;
        final List<YDBPermissionHolder.PermissionEntry> effectivePermissions;

        TransferInfo(
            @Nullable String sourcePath,
            @Nullable String destinationPath,
            @Nullable String sourceConnection,
            @Nullable String transformationLambda,
            @NotNull String state,
            @Nullable String consumerName,
            @Nullable String owner,
            @NotNull List<YDBPermissionHolder.PermissionEntry> permissions,
            @NotNull List<YDBPermissionHolder.PermissionEntry> effectivePermissions
        ) {
            this.sourcePath = sourcePath;
            this.destinationPath = destinationPath;
            this.sourceConnection = sourceConnection;
            this.transformationLambda = transformationLambda;
            this.state = state;
            this.consumerName = consumerName;
            this.owner = owner;
            this.permissions = permissions;
            this.effectivePermissions = effectivePermissions;
        }
    }

    /**
     * Describe a Transfer using raw gRPC Ydb.Replication.V1.ReplicationService/DescribeTransfer RPC.
     * The proto classes for this RPC are in draft and not available in the current shaded JAR,
     * so we encode/decode the request/response as raw bytes.
     *
     * <p>Request proto (Ydb.Replication.DescribeTransferRequest):</p>
     * <pre>
     * message DescribeTransferRequest {
     *     Ydb.Operations.OperationParams operation_params = 1;
     *     string path = 2;
     * }
     * </pre>
     *
     * <p>Result proto (Ydb.Replication.DescribeTransferResult):</p>
     * <pre>
     * message DescribeTransferResult {
     *     Ydb.Scheme.Entry self = 1;
     *     ConnectionParams connection_params = 2;
     *     oneof state {
     *         RunningState running = 3;
     *         ErrorState error = 4;
     *         DoneState done = 5;
     *         PausedState paused = 6;
     *     }
     *     string source_path = 7;
     *     string destination_path = 8;
     *     string transformation_lambda = 9;
     *     string consumer_name = 10;
     *     optional BatchSettings batch_settings = 11;
     * }
     * </pre>
     *
     * @return transfer info, or null if the RPC fails
     */
    @Nullable
    static TransferInfo describeTransfer(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        try {
            tech.ydb.shaded.grpc.MethodDescriptor<byte[], byte[]> method =
                tech.ydb.shaded.grpc.MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(tech.ydb.shaded.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("Ydb.Replication.V1.ReplicationService/DescribeTransfer")
                    .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
                    .setResponseMarshaller(ByteArrayMarshaller.INSTANCE)
                    .build();

            // Encode DescribeTransferRequest: field 2 (path) as length-delimited string
            byte[] pathBytes = path.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] request = new byte[1 + computeVarIntSize(pathBytes.length) + pathBytes.length];
            int offset = 0;
            request[offset++] = (byte) ((2 << 3) | 2); // field 2, wire type 2 (length-delimited)
            offset = writeVarInt(request, offset, pathBytes.length);
            System.arraycopy(pathBytes, 0, request, offset, pathBytes.length);

            GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();
            byte[] responseBytes = transport
                .unaryCall(method, settings, request)
                .join()
                .getValue();

            // Decode response: field 1 is Operation (length-delimited)
            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.debug("DescribeTransfer: no operation in response for " + path);
                return null;
            }

            // Check operation status: field 3 (StatusIds.StatusCode) is varint
            Integer statusValue = readVarIntField(operationBytes, 3);
            if (statusValue != null && statusValue != 400000 && statusValue != 0) {
                log.debug("DescribeTransfer: operation failed for " + path + ", status=" + statusValue);
                return null;
            }

            // Extract result from operation.result (field 5, google.protobuf.Any)
            byte[] anyBytes = readField(operationBytes, 5);
            if (anyBytes == null) {
                log.debug("DescribeTransfer: no result in operation for " + path);
                return null;
            }

            // google.protobuf.Any: field 2 = value (the serialized DescribeTransferResult)
            byte[] resultBytes = readField(anyBytes, 2);
            if (resultBytes == null) {
                log.debug("DescribeTransfer: no value in Any for " + path);
                return null;
            }

            // Parse DescribeTransferResult fields
            String sourcePath = readStringField(resultBytes, 7);
            String destinationPath = readStringField(resultBytes, 8);
            String transformationLambda = readStringField(resultBytes, 9);
            String consumerName = readStringField(resultBytes, 10);

            // Parse ConnectionParams (field 2): endpoint=1, database=2
            String sourceConnection = null;
            byte[] connBytes = readField(resultBytes, 2);
            if (connBytes != null) {
                String endpoint = readStringField(connBytes, 1);
                String database = readStringField(connBytes, 2);
                if (endpoint != null && !endpoint.isEmpty()) {
                    sourceConnection = endpoint + (database != null ? database : "");
                }
            }

            // Detect state from oneof: running=3, error=4, done=5, paused=6
            String state = "Unknown";
            if (readField(resultBytes, 3) != null) {
                state = "Running";
            } else if (readField(resultBytes, 4) != null) {
                state = "Error";
            } else if (readField(resultBytes, 5) != null) {
                state = "Done";
            } else if (readField(resultBytes, 6) != null) {
                state = "Paused";
            }

            // Parse self (field 1) for owner/permissions
            String owner = null;
            List<YDBPermissionHolder.PermissionEntry> permissions = List.of();
            List<YDBPermissionHolder.PermissionEntry> effectivePermissions = List.of();

            byte[] selfBytes = readField(resultBytes, 1);
            if (selfBytes != null) {
                owner = readStringField(selfBytes, 2);
                permissions = parsePermissionEntries(selfBytes, 7);
                effectivePermissions = parsePermissionEntries(selfBytes, 6);
            }

            return new TransferInfo(
                sourcePath, destinationPath, sourceConnection, transformationLambda, state,
                consumerName, owner, permissions, effectivePermissions);

        } catch (Exception e) {
            log.debug("DescribeTransfer error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Read a string field (length-delimited) from protobuf bytes.
     */
    @Nullable
    private static String readStringField(byte[] data, int fieldNumber) {
        byte[] bytes = readField(data, fieldNumber);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Read all occurrences of a length-delimited field from protobuf bytes.
     * Used for repeated fields like map entries.
     */
    @NotNull
    private static List<byte[]> readAllFields(byte[] data, int targetFieldNumber) {
        List<byte[]> results = new ArrayList<>();
        int pos = 0;
        while (pos < data.length) {
            int[] tagResult = readVarInt(data, pos);
            if (tagResult == null) break;
            int tag = tagResult[0];
            pos = tagResult[1];

            int fieldNumber = tag >>> 3;
            int wireType = tag & 0x7;

            switch (wireType) {
                case 0: // varint
                    int[] varIntResult = readVarInt(data, pos);
                    if (varIntResult == null) return results;
                    pos = varIntResult[1];
                    break;
                case 1: // 64-bit
                    pos += 8;
                    break;
                case 2: // length-delimited
                    int[] lenResult = readVarInt(data, pos);
                    if (lenResult == null) return results;
                    int len = lenResult[0];
                    pos = lenResult[1];
                    if (fieldNumber == targetFieldNumber) {
                        byte[] value = new byte[len];
                        System.arraycopy(data, pos, value, 0, len);
                        results.add(value);
                    }
                    pos += len;
                    break;
                case 5: // 32-bit
                    pos += 4;
                    break;
                default:
                    return results;
            }
        }
        return results;
    }

    /**
     * Read a protobuf map<string, string> field.
     * Each map entry is a repeated length-delimited message with key (field 1) and value (field 2).
     */
    @NotNull
    private static Map<String, String> readMapEntries(byte[] data, int mapFieldNumber) {
        Map<String, String> map = new LinkedHashMap<>();
        List<byte[]> entries = readAllFields(data, mapFieldNumber);
        for (byte[] entry : entries) {
            String key = readStringField(entry, 1);
            String value = readStringField(entry, 2);
            if (key != null) {
                map.put(key, value != null ? value : "");
            }
        }
        return map;
    }

    /**
     * Parse repeated Ydb.Scheme.Permissions messages from a parent protobuf message.
     * Permissions proto: field 1 = subject (string), field 2 = permission_names (repeated string).
     */
    @NotNull
    private static List<YDBPermissionHolder.PermissionEntry> parsePermissionEntries(byte[] data, int fieldNumber) {
        List<byte[]> permMsgs = readAllFields(data, fieldNumber);
        if (permMsgs.isEmpty()) {
            return List.of();
        }
        List<YDBPermissionHolder.PermissionEntry> result = new ArrayList<>();
        for (byte[] permMsg : permMsgs) {
            String subject = readStringField(permMsg, 1);
            if (subject != null) {
                List<byte[]> nameFields = readAllFields(permMsg, 2);
                List<String> names = new ArrayList<>();
                for (byte[] nameBytes : nameFields) {
                    names.add(new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8));
                }
                result.add(new YDBPermissionHolder.PermissionEntry(subject, names));
            }
        }
        return result;
    }

    /**
     * Read a length-delimited field from a protobuf message.
     * Returns the raw bytes of the field value, or null if not found.
     */
    @Nullable
    private static byte[] readField(byte[] data, int targetFieldNumber) {
        int pos = 0;
        while (pos < data.length) {
            int[] tagResult = readVarInt(data, pos);
            if (tagResult == null) break;
            int tag = tagResult[0];
            pos = tagResult[1];

            int fieldNumber = tag >>> 3;
            int wireType = tag & 0x7;

            switch (wireType) {
                case 0: // varint
                    int[] varIntResult = readVarInt(data, pos);
                    if (varIntResult == null) return null;
                    pos = varIntResult[1];
                    break;
                case 1: // 64-bit
                    pos += 8;
                    break;
                case 2: // length-delimited
                    int[] lenResult = readVarInt(data, pos);
                    if (lenResult == null) return null;
                    int len = lenResult[0];
                    pos = lenResult[1];
                    if (fieldNumber == targetFieldNumber) {
                        byte[] value = new byte[len];
                        System.arraycopy(data, pos, value, 0, len);
                        return value;
                    }
                    pos += len;
                    break;
                case 5: // 32-bit
                    pos += 4;
                    break;
                default:
                    return null; // unknown wire type
            }
        }
        return null;
    }

    /**
     * Read a varint field from a protobuf message.
     * Returns the integer value, or null if not found.
     */
    @Nullable
    private static Integer readVarIntField(byte[] data, int targetFieldNumber) {
        int pos = 0;
        while (pos < data.length) {
            int[] tagResult = readVarInt(data, pos);
            if (tagResult == null) break;
            int tag = tagResult[0];
            pos = tagResult[1];

            int fieldNumber = tag >>> 3;
            int wireType = tag & 0x7;

            switch (wireType) {
                case 0: // varint
                    int[] varIntResult = readVarInt(data, pos);
                    if (varIntResult == null) return null;
                    if (fieldNumber == targetFieldNumber) {
                        return varIntResult[0];
                    }
                    pos = varIntResult[1];
                    break;
                case 1: // 64-bit
                    pos += 8;
                    break;
                case 2: // length-delimited
                    int[] lenResult = readVarInt(data, pos);
                    if (lenResult == null) return null;
                    pos = lenResult[1] + lenResult[0];
                    break;
                case 5: // 32-bit
                    pos += 4;
                    break;
                default:
                    return null;
            }
        }
        return null;
    }

    private static int[] readVarInt(byte[] data, int pos) {
        int result = 0;
        int shift = 0;
        while (pos < data.length) {
            byte b = data[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return new int[]{result, pos};
            }
            shift += 7;
            if (shift >= 35) return null; // overflow for int
        }
        return null;
    }

    private static int computeVarIntSize(int value) {
        if (value < 0) return 5;
        if (value < (1 << 7)) return 1;
        if (value < (1 << 14)) return 2;
        if (value < (1 << 21)) return 3;
        if (value < (1 << 28)) return 4;
        return 5;
    }

    private static int writeVarInt(byte[] buf, int pos, int value) {
        while ((value & ~0x7F) != 0) {
            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        return pos;
    }

    /**
     * Marshaller for raw byte arrays, used for gRPC calls where proto classes are not available.
     */
    private static final class ByteArrayMarshaller implements tech.ydb.shaded.grpc.MethodDescriptor.Marshaller<byte[]> {
        static final ByteArrayMarshaller INSTANCE = new ByteArrayMarshaller();

        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static int getPrimitivePrecision(@NotNull ValueProtos.Type.PrimitiveTypeId typeId) {
        switch (typeId) {
            case BOOL:
            case INT8:
            case UINT8:
                return 1;
            case INT16:
            case UINT16:
                return 2;
            case INT32:
            case UINT32:
            case FLOAT:
                return 4;
            case INT64:
            case UINT64:
            case DOUBLE:
            case INTERVAL:
                return 8;
            case UUID:
                return 16;
            case DATE:
                return 10;
            case DATETIME:
                return 19;
            case TIMESTAMP:
                return 26;
            case TZ_DATE:
                return 16;
            case TZ_DATETIME:
                return 25;
            case TZ_TIMESTAMP:
                return 32;
            case UTF8:
            case STRING:
            case JSON:
            case JSON_DOCUMENT:
            case YSON:
                return 0; // variable length
            case DYNUMBER:
                return 0;
            default:
                return 0;
        }
    }
}
