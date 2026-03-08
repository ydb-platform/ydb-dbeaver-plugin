package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.scheme.SchemeClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class YDBBaseIT {

    protected static final String DEFAULT_URL = "jdbc:ydb:grpc://localhost:2136/local";

    protected Connection connection;
    protected SchemeClient schemeClient;
    protected GrpcTransport grpcTransport;
    protected String prefixPath;

    @BeforeAll
    void openConnection() throws SQLException {
        String url = System.getProperty("ydb.jdbc.url", DEFAULT_URL);
        connection = DriverManager.getConnection(url);
        if (connection.isWrapperFor(YdbConnection.class)) {
            YdbConnection ydbConn = connection.unwrap(YdbConnection.class);
            YdbContext ctx = ydbConn.getCtx();
            schemeClient = ctx.getSchemeClient();
            grpcTransport = ctx.getGrpcTransport();
            prefixPath = ctx.getPrefixPath();
        }
    }

    @AfterAll
    void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
