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
import org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * DBeaver-layer wrapper around YDB RPC calls.
 *
 * <p>Raw gRPC wrappers (describeExternalTable, describeExternalDataSource,
 * describeTransfer, describeViewQueryText) live in {@link YDBGrpcHelper} so that
 * integration tests can use the same code without a DBeaver dependency.
 * This class delegates to {@link YDBGrpcHelper} and converts its
 * {@link YDBGrpcHelper.PermissionEntry} results to {@link YDBPermissionHolder.PermissionEntry}.
 *
 * <h3>TODO: migrate to dedicated DescribeExternal* RPCs</h3>
 * <p>
 * Currently we use raw bytes because the YDB Java SDK ({@code ydb-proto-api}) does not yet
 * include the dedicated proto messages. When a new version adds them, refactor as follows:
 * </p>
 * <ol>
 *   <li>Check: {@code jar tf ydb-jdbc-driver-shaded-X.Y.Z.jar | grep DescribeExternal}</li>
 *   <li>Update {@link YDBGrpcHelper} to use the new proto-generated stubs instead of raw bytes.</li>
 *   <li>Update plugin.xml driver version.</li>
 * </ol>
 */
final class YDBDescribeHelper {

    private static final Log log = Log.getLog(YDBDescribeHelper.class);

    private YDBDescribeHelper() {
    }

    // -------------------------------------------------------------------------
    // High-level SDK calls
    // -------------------------------------------------------------------------

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
     * Call SchemeClient.describePath() and return the raw proto Entry.
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

    // -------------------------------------------------------------------------
    // Raw gRPC delegations — implemented in YDBGrpcHelper (DBeaver-independent)
    // -------------------------------------------------------------------------

    @Nullable
    static YDBGrpcHelper.ExternalDataSourceInfo describeExternalDataSource(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        return YDBGrpcHelper.describeExternalDataSource(transport, path);
    }

    @Nullable
    static YDBGrpcHelper.ExternalTableInfo describeExternalTable(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        return YDBGrpcHelper.describeExternalTable(transport, path);
    }

    @Nullable
    static YDBGrpcHelper.TransferInfo describeTransfer(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        return YDBGrpcHelper.describeTransfer(transport, path);
    }

    @Nullable
    static String describeViewQueryText(
        @NotNull GrpcTransport transport,
        @NotNull String path
    ) {
        return YDBGrpcHelper.describeViewQueryText(transport, path);
    }

    // -------------------------------------------------------------------------
    // Permission conversions and helpers
    // -------------------------------------------------------------------------

    /**
     * Convert {@link YDBGrpcHelper.PermissionEntry} (DBeaver-independent DTO) to
     * {@link YDBPermissionHolder.PermissionEntry} (DBeaver model type).
     */
    @NotNull
    static List<YDBPermissionHolder.PermissionEntry> convertPermissions(
        @NotNull List<YDBGrpcHelper.PermissionEntry> entries
    ) {
        List<YDBPermissionHolder.PermissionEntry> result = new ArrayList<>(entries.size());
        for (YDBGrpcHelper.PermissionEntry e : entries) {
            result.add(new YDBPermissionHolder.PermissionEntry(e.subject, e.permissionNames));
        }
        return result;
    }

    /**
     * Convert proto Permissions messages to DBeaver PermissionEntry list.
     * Used when reading permissions from a SchemeClient.describePath() result.
     */
    @NotNull
    static List<YDBPermissionHolder.PermissionEntry> toPermissionEntries(
        @NotNull List<SchemeOperationProtos.Permissions> permissionsList
    ) {
        List<YDBPermissionHolder.PermissionEntry> result = new ArrayList<>();
        for (SchemeOperationProtos.Permissions perm : permissionsList) {
            result.add(new YDBPermissionHolder.PermissionEntry(
                perm.getSubject(),
                new ArrayList<>(perm.getPermissionNamesList())
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
     * Call SchemeService.ModifyPermissions RPC directly via GrpcTransport.
     * The Java SDK's SchemeClient does not expose this method.
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
            List<SchemeOperationProtos.PermissionsAction> protoActions = new ArrayList<>();
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

    // -------------------------------------------------------------------------
    // Type resolution helpers (used by plugin model classes, not by tests)
    // -------------------------------------------------------------------------

    static boolean isOptionalType(@NotNull ValueProtos.Type type) {
        return type.getTypeCase() == ValueProtos.Type.TypeCase.OPTIONAL_TYPE;
    }

    @NotNull
    static ValueProtos.Type unwrapOptionalType(@NotNull ValueProtos.Type type) {
        if (type.getTypeCase() == ValueProtos.Type.TypeCase.OPTIONAL_TYPE) {
            return type.getOptionalType().getItem();
        }
        return type;
    }

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
            default:
                return 0;
        }
    }
}
