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
import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;
import org.jkiss.dbeaver.model.DBPKeywordType;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCDatabaseMetaData;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSource;
import org.jkiss.dbeaver.model.sql.SQLDialect;

import java.util.Arrays;

public class YDBSQLDialect extends GenericSQLDialect {

    private static final String[][] STRING_QUOTES = {{"'", "'"}, {"\"", "\""}};
    private static final String[] SINGLE_LINE_COMMENTS = {"--"};
    private static final String[] PARAMETER_PREFIXES = {"$"};
    private static final String[][] BLOCK_BOUND_STRINGS = {{"BEGIN", "END"}};
    private static final String[] BLOCK_HEADER_STRINGS = {"DEFINE", "DO"};
    private static final String[] DDL_KEYWORDS = {"CREATE", "ALTER", "DROP", "UPSERT"};
    private static final String[] DML_KEYWORDS = {"INSERT", "DELETE", "UPDATE", "UPSERT", "REPLACE"};
    private static final String[] EXEC_KEYWORDS = {"DO", "EVALUATE"};
    private static final String[] COMMIT_KEYWORDS = {"COMMIT"};
    private static final String[] ROLLBACK_KEYWORDS = {"ROLLBACK"};

    public YDBSQLDialect() {
        super("YDB", "ydb");
    }

    public void initDriverSettings(JDBCSession session, JDBCDataSource dataSource, JDBCDatabaseMetaData metaData) {
        if (metaData != null) {
            super.initDriverSettings(session, dataSource, metaData);
        }
        addKeywords(Arrays.asList(
            "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANSI", "ANY", "ARRAY", "AS", "ASC",
            "ASSUME", "ASYMMETRIC", "ASYNC", "AT", "ATTACH", "ATTRIBUTES", "AUTOINCREMENT", "AUTOMAP",
            "BACKUP", "BATCH", "BEFORE", "BEGIN", "BERNOULLI", "BETWEEN", "BITCAST", "BY",
            "CALLABLE", "CASCADE", "CASE", "CAST", "CHANGEFEED", "CHECK", "CLASSIFIER", "COLLATE",
            "COLLECTION", "COLUMN", "COLUMNS", "COMMIT", "COMPACT", "CONDITIONAL", "CONFLICT", "CONNECT",
            "CONSTRAINT", "CONSUMER", "COVER", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "DATA", "DATABASE", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DEFINE",
            "DELETE", "DESC", "DESCRIBE", "DETACH", "DICT", "DIRECTORY", "DISABLE", "DISCARD", "DISTINCT", "DO",
            "DROP",
            "EACH", "ELSE", "EMPTY", "EMPTY_ACTION", "ENCRYPTED", "END", "ENUM", "ERASE", "ERROR", "ESCAPE",
            "EVALUATE", "EXCEPT", "EXCLUDE", "EXCLUSION", "EXCLUSIVE", "EXISTS", "EXPLAIN", "EXPORT", "EXTERNAL",
            "FAIL", "FALSE", "FAMILY", "FILTER", "FIRST", "FLATTEN", "FLOW", "FOLLOWING", "FOR", "FOREIGN", "FROM",
            "FULL", "FUNCTION",
            "GLOB", "GLOBAL", "GRANT", "GROUP", "GROUPING", "GROUPS",
            "HASH", "HAVING", "HOP",
            "IF", "IGNORE", "ILIKE", "IMMEDIATE", "IMPORT", "IN", "INCREMENT", "INCREMENTAL", "INDEX", "INDEXED",
            "INHERITS", "INITIAL", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL",
            "JOIN", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE",
            "KEY",
            "LAST", "LEFT", "LEGACY", "LIKE", "LIMIT", "LIST", "LOCAL", "LOGIN",
            "MANAGE", "MATCH", "MATCHES", "MATCH_RECOGNIZE", "MEASURES", "MICROSECONDS", "MILLISECONDS", "MODIFY",
            "NANOSECONDS", "NATURAL", "NEXT", "NO", "NOLOGIN", "NOT", "NOTNULL", "NULL", "NULLS",
            "OBJECT", "OF", "OFFSET", "OMIT", "ON", "ONE", "ONLY", "OPTION", "OPTIONAL", "OR", "ORDER", "OTHERS",
            "OUTER", "OVER", "OWNER",
            "PARALLEL", "PARTITION", "PASSING", "PASSWORD", "PAST", "PATTERN", "PER", "PERMUTE", "PLAN", "POOL",
            "PRAGMA", "PRECEDING", "PRESORT", "PRIMARY", "PRIVILEGES", "PROCESS",
            "QUERY", "QUEUE",
            "RAISE", "RANGE", "REDUCE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "REMOVE", "RENAME",
            "REPEATABLE", "REPLACE", "REPLICATION", "RESET", "RESOURCE", "RESPECT", "RESTART", "RESTORE", "RESTRICT",
            "RESULT", "RETURN", "RETURNING", "REVERT", "REVOKE", "RIGHT", "RLIKE", "ROLLBACK", "ROLLUP", "ROW",
            "ROWS",
            "SAMPLE", "SAVEPOINT", "SCHEMA", "SECONDS", "SECRET", "SEEK", "SELECT", "SEMI", "SEQUENCE", "SET",
            "SETS", "SHOW", "TSKIP", "SOURCE", "START", "STREAM", "STREAMING", "STRUCT", "SUBQUERY", "SUBSET",
            "SYMBOLS", "SYMMETRIC", "SYNC", "SYSTEM",
            "TABLE", "TABLES", "TABLESAMPLE", "TABLESTORE", "TAGGED", "TEMP", "TEMPORARY", "THEN", "TIES", "TO",
            "TOPIC", "TRANSACTION", "TRANSFER", "TRIGGER", "TRUE", "TUPLE", "TYPE",
            "UNBOUNDED", "UNCONDITIONAL", "UNION", "UNIQUE", "UNKNOWN", "UNMATCHED", "UPDATE", "UPSERT", "USE",
            "USER", "USING",
            "VACUUM", "VALUES", "VARIANT", "VIEW", "VIRTUAL",
            "WATERMARK", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT", "WRAPPER",
            "XOR"
        ), DBPKeywordType.KEYWORD);
        addDataTypes(Arrays.asList(
            "BOOL", "BOOLEAN", "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32", "INT64", "UINT64",
            "FLOAT", "DOUBLE", "STRING", "UTF8", "YSON", "JSON", "UUID", "DATE", "DATETIME", "TIMESTAMP",
            "INTERVAL", "TZ_DATE", "TZ_DATETIME", "TZ_TIMESTAMP", "OPTIONAL", "LIST", "DICT", "SET", "TUPLE",
            "STRUCT", "VARIANT", "ENUM", "RESOURCE", "TAGGED", "CALLABLE", "FLOW",
            "DYNUMBER", "JSONDOCUMENT", "TEXT", "BYTES", "DATE32", "DATETIME64", "TIMESTAMP64", "INTERVAL64"
        ));
        addFunctions(Arrays.asList(
            // String
            "Length", "Substring", "Find", "RFind", "StartsWith", "EndsWith", "StringReverse", "StringContains",
            "StringReplace", "StringSplitToList", "StringJoinFromList", "StringIsAscii", "StringIsUTF8",
            "StringIsAlNum", "StringIsAlpha", "StringIsDigit", "StringIsLower", "StringIsUpper", "StringIsSpace",
            "StringIsTitle", "StringToLower", "StringToUpper", "StringStrip", "StringLStrip", "StringRStrip",
            "StringRandom", "StringHash", "StringFNV", "StringCrc32", "StringCrc64", "StringLike", "StringMatch",
            // Datetime
            "CurrentUtc", "CurrentTz", "Now", "TimestampFromIso", "TimestampFromSeconds", "TimestampToSeconds",
            "DateFromDays", "DateToDays", "DateComponent", "TimeComponent", "DateTimeComponent", "TimezoneId",
            "TimezoneName", "AddTime", "SubTime", "AbsTime",
            "RemoveTimezoneFromTimestamp", "AddTimezone",
            // Math
            "Abs", "Acos", "Asin", "Atan", "Atan2", "Cbrt", "Ceil", "Cos", "Cosh", "Erf", "Exp", "Exp2", "Fabs",
            "Floor", "Fmod", "Hypot", "Lgamma", "Log", "Log2", "Log10", "Mod", "Round", "Sin", "Sinh", "Sqrt", "Tan",
            "Tanh", "Trunc",
            // Aggregate
            "COUNT", "COUNT_IF", "SUM", "AVG", "MIN", "MAX", "SOME", "EVERY",
            "VARIANCE", "STDDEV", "CORRELATION", "COVARIANCE",
            "AGGREGATE_LIST", "AGGREGATE_LIST_DISTINCT",
            "TOP", "BOTTOM", "TOPFREQ",
            "HISTOGRAM", "LinearHistogram", "LogHistogram", "LogarithmicHistogram",
            "HyperLogLog", "CountDistinctEstimate", "HLL",
            "Percentile", "Median",
            "CountMinSketch",
            "MAX_OF", "MIN_OF", "GREATEST", "LEAST",
            // Conditional
            "Coalesce", "NVL", "NANVL", "IF",
            // Type inspection and casting
            "EnsureType", "EnsureConvertibleTo", "AssumeStrict", "Likely",
            "Ensure", "Unwrap", "Just", "Nothing",
            "Pickle", "Unpickle",
            "WeakField",
            // Container - List
            "ListCreate", "ListLength", "ListCollect", "ListFlatMap", "ListMap", "ListFilter",
            "ListReverse", "ListSkip", "ListTake", "ListHead", "ListLast", "ListEnumerate",
            "ListMin", "ListMax", "ListSum", "ListAvg",
            "ListFold", "ListFold1", "ListFold1Map", "ListFoldMap",
            "ListAll", "ListAny", "ListZip", "ListZipAll",
            "ListFromRange", "ListReplicate", "ListNotNull", "ListFlatten",
            "ListConcat", "ListSort", "ListSortAsc", "ListSortDesc", "ListHas", "ListHasAny", "ListHasAll",
            "ListUniq", "ListExtend", "ListUnion", "ListIntersection", "ListDifference", "ListSymmetricDifference",
            // Container - Dict
            "DictCreate", "DictLength", "DictKeys", "DictPayloads", "DictItems",
            "DictLookup", "DictContains", "DictAggregate",
            // Container - Set
            "ToSet", "SetIsDisjoint", "SetIntersection", "SetIncludes",
            "SetUnion", "SetDifference", "SetSymmetricDifference",
            // Struct
            "StaticMap", "StaticZip",
            // Table
            "TableRow", "TableName", "TablePath", "TableRecordIndex",
            // Window
            "GROUPING", "SessionStart", "SessionState",
            // Other
            "Random", "RandomNumber", "RandomUuid", "Uuid", "AsTuple", "AsStruct", "AsList", "AsDict", "AsSet",
            "AsTagged", "Cast", "BitCast", "TypeOf", "IsSame", "FromBytes", "ToBytes"
        ));
    }

    @Override
    public String[][] getIdentifierQuoteStrings() {
        return new String[][]{{"`", "`"}};
    }

    @Override
    public boolean supportsUnquotedMixedCase() {
        return false;
    }

    @NotNull
    @Override
    public String[][] getStringQuoteStrings() {
        return STRING_QUOTES;
    }

    @Override
    public String[] getSingleLineComments() {
        return SINGLE_LINE_COMMENTS;
    }

    @NotNull
    @Override
    public String[] getParametersPrefixes() {
        return PARAMETER_PREFIXES;
    }

    @Nullable
    @Override
    public String[][] getBlockBoundStrings() {
        return BLOCK_BOUND_STRINGS;
    }

    @Nullable
    @Override
    public String[] getBlockHeaderStrings() {
        return BLOCK_HEADER_STRINGS;
    }

    @Override
    public boolean supportsSubqueries() {
        return true;
    }

    @Override
    public boolean supportsAliasInSelect() {
        return true;
    }

    @Override
    public boolean supportsAliasInConditions() {
        return false;
    }

    @Override
    public boolean supportsTableDropCascade() {
        return true;
    }

    @Override
    public boolean supportsOrderByIndex() {
        return false;
    }

    @Override
    public boolean supportsInsertAllDefaultValuesStatement() {
        return true;
    }

    @Override
    public boolean supportsCommentQuery() {
        return true;
    }

    @Override
    public boolean supportsNestedComments() {
        return true;
    }

    @Nullable
    @Override
    public String[] getTransactionCommitKeywords() {
        return COMMIT_KEYWORDS;
    }

    @Nullable
    @Override
    public String[] getTransactionRollbackKeywords() {
        return ROLLBACK_KEYWORDS;
    }

    @NotNull
    @Override
    public String[] getDDLKeywords() {
        return DDL_KEYWORDS;
    }

    @NotNull
    @Override
    public String[] getDMLKeywords() {
        return DML_KEYWORDS;
    }

    @NotNull
    @Override
    public String[] getExecuteKeywords() {
        return EXEC_KEYWORDS;
    }

    @Override
    public boolean validIdentifierStart(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    @Override
    public boolean validIdentifierPart(char c, boolean quoted) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
    }

    @Nullable
    @Override
    public String getTestSQL() {
        return "SELECT 1";
    }

    @NotNull
    @Override
    public SQLDialect.MultiValueInsertMode getDefaultMultiValueInsertMode() {
        return SQLDialect.MultiValueInsertMode.GROUP_ROWS;
    }

    @Override
    public char getStringEscapeCharacter() {
        return '\\';
    }
}
