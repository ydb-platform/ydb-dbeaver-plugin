package org.jkiss.dbeaver.ext.ydb.core;

import tech.ydb.core.grpc.GrpcRequestSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.table.YdbTable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Raw gRPC wrappers for YDB RPCs that are not yet exposed in the Java SDK.
 *
 * <p>This class is DBeaver-independent: it only depends on the YDB JDBC shaded JAR
 * and standard Java. It is shared between the OSGi plugin and integration tests so
 * that both exercise exactly the same RPC logic.</p>
 *
 * <p>The plugin's {@code YDBDescribeHelper} delegates to this class and wraps the
 * results in DBeaver model types (converting {@link PermissionEntry} to
 * {@code YDBPermissionHolder.PermissionEntry}).</p>
 */
public final class YDBGrpcHelper {

    private static final Logger log = Logger.getLogger(YDBGrpcHelper.class.getName());

    private YDBGrpcHelper() {
    }

    // -------------------------------------------------------------------------
    // Public DTOs
    // -------------------------------------------------------------------------

    /** A single ACL entry: a subject (SID) and its granted permission names. */
    public static final class PermissionEntry {
        public final String subject;
        public final List<String> permissionNames;

        public PermissionEntry(String subject, List<String> permissionNames) {
            this.subject = subject;
            this.permissionNames = permissionNames;
        }
    }

    /** Result of {@link #describeExternalDataSource}. */
    public static final class ExternalDataSourceInfo {
        public final String sourceType;
        public final String location;
        public final Map<String, String> properties;
        public final String owner;
        public final List<PermissionEntry> permissions;
        public final List<PermissionEntry> effectivePermissions;

        public ExternalDataSourceInfo(
            String sourceType,
            String location,
            Map<String, String> properties,
            String owner,
            List<PermissionEntry> permissions,
            List<PermissionEntry> effectivePermissions
        ) {
            this.sourceType = sourceType;
            this.location = location;
            this.properties = properties;
            this.owner = owner;
            this.permissions = permissions;
            this.effectivePermissions = effectivePermissions;
        }
    }

    /** Result of {@link #describeExternalTable}. */
    public static final class ExternalTableInfo {
        public final String sourceType;
        public final String dataSourcePath;
        public final String location;
        /** Parsed proto {@code ColumnMeta} objects; use {@code getName()} and {@code getType()}. */
        public final List<YdbTable.ColumnMeta> columns;
        public final Map<String, String> content;
        public final String owner;
        public final List<PermissionEntry> permissions;
        public final List<PermissionEntry> effectivePermissions;

        public ExternalTableInfo(
            String sourceType,
            String dataSourcePath,
            String location,
            List<YdbTable.ColumnMeta> columns,
            Map<String, String> content,
            String owner,
            List<PermissionEntry> permissions,
            List<PermissionEntry> effectivePermissions
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

    /** Result of {@link #describeTransfer}. */
    public static final class TransferInfo {
        public final String sourcePath;
        public final String destinationPath;
        public final String sourceConnection;
        public final String transformationLambda;
        public final String state;
        public final String consumerName;
        public final String owner;
        public final List<PermissionEntry> permissions;
        public final List<PermissionEntry> effectivePermissions;

        public TransferInfo(
            String sourcePath,
            String destinationPath,
            String sourceConnection,
            String transformationLambda,
            String state,
            String consumerName,
            String owner,
            List<PermissionEntry> permissions,
            List<PermissionEntry> effectivePermissions
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

    // -------------------------------------------------------------------------
    // Public RPC methods
    // -------------------------------------------------------------------------

    /**
     * Describe an External Data Source via raw gRPC
     * {@code Ydb.Table.V1.TableService/DescribeExternalDataSource}.
     *
     * <p>Proto result (DescribeExternalDataSourceResult):
     * <pre>
     *   field 1  Ydb.Scheme.Entry self
     *   field 2  string source_type
     *   field 3  string location
     *   field 4  map&lt;string,string&gt; properties
     * </pre>
     *
     * @return parsed info, or {@code null} if the RPC fails
     */
    public static ExternalDataSourceInfo describeExternalDataSource(
        GrpcTransport transport,
        String path
    ) {
        try {
            byte[] responseBytes = rawUnaryCall(transport,
                "Ydb.Table.V1.TableService/DescribeExternalDataSource", path);
            if (responseBytes == null) {
                return null;
            }

            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.fine("DescribeExternalDataSource: no operation in response for " + path);
                return null;
            }
            if (!isOperationSuccess(operationBytes)) {
                return null;
            }

            byte[] resultBytes = extractResultBytes(operationBytes, path, "DescribeExternalDataSource");
            if (resultBytes == null) {
                return null;
            }

            String sourceType = readStringField(resultBytes, 2);
            String location = readStringField(resultBytes, 3);
            Map<String, String> properties = readMapEntries(resultBytes, 4);

            String owner = null;
            List<PermissionEntry> permissions = List.of();
            List<PermissionEntry> effectivePermissions = List.of();
            byte[] selfBytes = readField(resultBytes, 1);
            if (selfBytes != null) {
                owner = readStringField(selfBytes, 2);
                permissions = parsePermissionEntries(selfBytes, 7);
                effectivePermissions = parsePermissionEntries(selfBytes, 6);
            }

            return new ExternalDataSourceInfo(sourceType, location, properties,
                owner, permissions, effectivePermissions);

        } catch (Exception e) {
            log.fine("DescribeExternalDataSource error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Describe an External Table via raw gRPC
     * {@code Ydb.Table.V1.TableService/DescribeExternalTable}.
     *
     * <p>Proto result (DescribeExternalTableResult):
     * <pre>
     *   field 1  Ydb.Scheme.Entry self
     *   field 2  string source_type
     *   field 3  string data_source_path
     *   field 4  string location
     *   field 5  repeated ColumnMeta columns
     *   field 6  map&lt;string,string&gt; content
     * </pre>
     *
     * @return parsed info, or {@code null} if the RPC fails
     */
    public static ExternalTableInfo describeExternalTable(
        GrpcTransport transport,
        String path
    ) {
        try {
            byte[] responseBytes = rawUnaryCall(transport,
                "Ydb.Table.V1.TableService/DescribeExternalTable", path);
            if (responseBytes == null) {
                return null;
            }

            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.fine("DescribeExternalTable: no operation in response for " + path);
                return null;
            }
            if (!isOperationSuccess(operationBytes)) {
                return null;
            }

            byte[] resultBytes = extractResultBytes(operationBytes, path, "DescribeExternalTable");
            if (resultBytes == null) {
                return null;
            }

            String sourceType = readStringField(resultBytes, 2);
            String dataSourcePath = readStringField(resultBytes, 3);
            String location = readStringField(resultBytes, 4);
            Map<String, String> content = readMapEntries(resultBytes, 6);

            List<byte[]> columnRawList = readAllFields(resultBytes, 5);
            List<YdbTable.ColumnMeta> columns = new ArrayList<>();
            for (byte[] colBytes : columnRawList) {
                try {
                    columns.add(YdbTable.ColumnMeta.parseFrom(colBytes));
                } catch (tech.ydb.shaded.google.protobuf.InvalidProtocolBufferException e) {
                    log.fine("Failed to parse ColumnMeta for " + path + ": " + e.getMessage());
                }
            }

            String owner = null;
            List<PermissionEntry> permissions = List.of();
            List<PermissionEntry> effectivePermissions = List.of();
            byte[] selfBytes = readField(resultBytes, 1);
            if (selfBytes != null) {
                owner = readStringField(selfBytes, 2);
                permissions = parsePermissionEntries(selfBytes, 7);
                effectivePermissions = parsePermissionEntries(selfBytes, 6);
            }

            return new ExternalTableInfo(sourceType, dataSourcePath, location, columns, content,
                owner, permissions, effectivePermissions);

        } catch (Exception e) {
            log.fine("DescribeExternalTable error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Describe a Transfer via raw gRPC
     * {@code Ydb.Replication.V1.ReplicationService/DescribeTransfer}.
     *
     * <p>Proto result (DescribeTransferResult):
     * <pre>
     *   field 1   Ydb.Scheme.Entry self
     *   field 2   ConnectionParams connection_params (endpoint=1, database=2)
     *   field 3   RunningState running
     *   field 4   ErrorState error
     *   field 5   DoneState done
     *   field 6   PausedState paused
     *   field 7   string source_path
     *   field 8   string destination_path
     *   field 9   string transformation_lambda
     *   field 10  string consumer_name
     * </pre>
     *
     * @return parsed info, or {@code null} if the RPC fails
     */
    public static TransferInfo describeTransfer(
        GrpcTransport transport,
        String path
    ) {
        try {
            byte[] responseBytes = rawUnaryCall(transport,
                "Ydb.Replication.V1.ReplicationService/DescribeTransfer", path);
            if (responseBytes == null) {
                return null;
            }

            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.fine("DescribeTransfer: no operation in response for " + path);
                return null;
            }
            if (!isOperationSuccess(operationBytes)) {
                return null;
            }

            byte[] resultBytes = extractResultBytes(operationBytes, path, "DescribeTransfer");
            if (resultBytes == null) {
                return null;
            }

            String sourcePath = readStringField(resultBytes, 7);
            String destinationPath = readStringField(resultBytes, 8);
            String transformationLambda = readStringField(resultBytes, 9);
            String consumerName = readStringField(resultBytes, 10);

            String sourceConnection = null;
            byte[] connBytes = readField(resultBytes, 2);
            if (connBytes != null) {
                String endpoint = readStringField(connBytes, 1);
                String database = readStringField(connBytes, 2);
                if (endpoint != null && !endpoint.isEmpty()) {
                    sourceConnection = endpoint + (database != null ? database : "");
                }
            }

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

            String owner = null;
            List<PermissionEntry> permissions = List.of();
            List<PermissionEntry> effectivePermissions = List.of();
            byte[] selfBytes = readField(resultBytes, 1);
            if (selfBytes != null) {
                owner = readStringField(selfBytes, 2);
                permissions = parsePermissionEntries(selfBytes, 7);
                effectivePermissions = parsePermissionEntries(selfBytes, 6);
            }

            return new TransferInfo(sourcePath, destinationPath, sourceConnection,
                transformationLambda, state, consumerName, owner, permissions, effectivePermissions);

        } catch (Exception e) {
            log.fine("DescribeTransfer error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Describe a View via raw gRPC {@code Ydb.View.V1.ViewService/DescribeView}.
     *
     * <p>Proto result (DescribeViewResult):
     * <pre>
     *   field 1  Ydb.Scheme.Entry self
     *   field 2  string query_text
     * </pre>
     *
     * @return the view's query text, or {@code null} if the RPC fails
     */
    public static String describeViewQueryText(
        GrpcTransport transport,
        String path
    ) {
        try {
            byte[] responseBytes = rawUnaryCall(transport, "Ydb.View.V1.ViewService/DescribeView", path);
            if (responseBytes == null) {
                return null;
            }

            byte[] operationBytes = readField(responseBytes, 1);
            if (operationBytes == null) {
                log.fine("DescribeView: no operation in response for " + path);
                return null;
            }
            if (!isOperationSuccess(operationBytes)) {
                return null;
            }

            byte[] resultBytes = extractResultBytes(operationBytes, path, "DescribeView");
            if (resultBytes == null) {
                return null;
            }

            byte[] queryTextBytes = readField(resultBytes, 2);
            if (queryTextBytes == null) {
                return "";
            }
            return new String(queryTextBytes, StandardCharsets.UTF_8);

        } catch (Exception e) {
            log.fine("DescribeView error for " + path + ": " + e.getMessage());
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Send a unary gRPC call with a single {@code string path} field (field 2).
     * Returns the raw response bytes, or {@code null} on error.
     */
    private static byte[] rawUnaryCall(GrpcTransport transport, String fullMethodName, String path) {
        try {
            tech.ydb.shaded.grpc.MethodDescriptor<byte[], byte[]> method =
                tech.ydb.shaded.grpc.MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(tech.ydb.shaded.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(fullMethodName)
                    .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
                    .setResponseMarshaller(ByteArrayMarshaller.INSTANCE)
                    .build();

            byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
            byte[] request = new byte[1 + computeVarIntSize(pathBytes.length) + pathBytes.length];
            int offset = 0;
            request[offset++] = (byte) ((2 << 3) | 2); // field 2, wire type 2 (length-delimited)
            offset = writeVarInt(request, offset, pathBytes.length);
            System.arraycopy(pathBytes, 0, request, offset, pathBytes.length);

            GrpcRequestSettings settings = GrpcRequestSettings.newBuilder().build();
            return transport.unaryCall(method, settings, request).join().getValue();
        } catch (Exception e) {
            log.fine(fullMethodName + " call failed for " + path + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Check the operation status field (field 3, varint).
     * SUCCESS = 400000, UNSPECIFIED = 0 (also treated as success).
     */
    private static boolean isOperationSuccess(byte[] operationBytes) {
        Integer statusValue = readVarIntField(operationBytes, 3);
        if (statusValue != null && statusValue != 400000 && statusValue != 0) {
            log.fine("Operation failed with status=" + statusValue);
            return false;
        }
        return true;
    }

    /**
     * Extract the result bytes from an Operation message.
     * Operation.result is a google.protobuf.Any (field 5):
     *   Any.value (field 2) is the serialized result message.
     */
    private static byte[] extractResultBytes(byte[] operationBytes, String path, String rpcName) {
        byte[] anyBytes = readField(operationBytes, 5);
        if (anyBytes == null) {
            log.fine(rpcName + ": no result in operation for " + path);
            return null;
        }
        byte[] resultBytes = readField(anyBytes, 2);
        if (resultBytes == null) {
            log.fine(rpcName + ": no value in Any for " + path);
            return null;
        }
        return resultBytes;
    }

    /** Parse repeated Ydb.Scheme.Permissions messages (subject=field1, permission_names=repeated field2). */
    private static List<PermissionEntry> parsePermissionEntries(byte[] data, int fieldNumber) {
        List<byte[]> permMsgs = readAllFields(data, fieldNumber);
        if (permMsgs.isEmpty()) {
            return List.of();
        }
        List<PermissionEntry> result = new ArrayList<>();
        for (byte[] permMsg : permMsgs) {
            String subject = readStringField(permMsg, 1);
            if (subject != null) {
                List<byte[]> nameFields = readAllFields(permMsg, 2);
                List<String> names = new ArrayList<>();
                for (byte[] nameBytes : nameFields) {
                    names.add(new String(nameBytes, StandardCharsets.UTF_8));
                }
                result.add(new PermissionEntry(subject, names));
            }
        }
        return result;
    }

    /** Read a {@code string} (length-delimited) field from protobuf bytes. */
    private static String readStringField(byte[] data, int fieldNumber) {
        byte[] bytes = readField(data, fieldNumber);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Read a {@code map<string,string>} field (each entry is a repeated message with key=1, value=2). */
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

    /** Read all occurrences of a length-delimited field (for repeated fields). */
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

    /** Read the first occurrence of a length-delimited field; returns null if not found. */
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
                    return null;
            }
        }
        return null;
    }

    /** Read a varint field; returns null if not found. */
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
            if (shift >= 35) return null;
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

    /** Marshaller for raw byte arrays, used when proto classes are not in the shaded JAR. */
    private static final class ByteArrayMarshaller
        implements tech.ydb.shaded.grpc.MethodDescriptor.Marshaller<byte[]> {

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
}
