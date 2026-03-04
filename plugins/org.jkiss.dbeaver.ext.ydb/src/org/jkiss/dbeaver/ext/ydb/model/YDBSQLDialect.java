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
import org.jkiss.dbeaver.ext.ydb.model.autocomplete.AutocompleteEntity;
import org.jkiss.dbeaver.ext.ydb.model.autocomplete.YDBAutocompleteClient;
import org.jkiss.dbeaver.model.DBPKeywordType;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCDatabaseMetaData;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSource;
import org.jkiss.dbeaver.model.sql.SQLDialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    private JDBCDataSource dataSource;
    private final Set<String> dynamicEntityNames = ConcurrentHashMap.newKeySet();

    public YDBSQLDialect() {
        super("YDB", "ydb");
    }

    public void initDriverSettings(JDBCSession session, JDBCDataSource dataSource, JDBCDatabaseMetaData metaData) {
        this.dataSource = dataSource;
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
            "XOR",
            // Pragmas
            "TablePathPrefix", "Warning"
        ), DBPKeywordType.KEYWORD);
        addDataTypes(Arrays.asList(
            "BOOL", "BOOLEAN", "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32", "INT64", "UINT64",
            "FLOAT", "DOUBLE", "STRING", "UTF8", "YSON", "JSON", "UUID", "DATE", "DATETIME", "TIMESTAMP",
            "INTERVAL", "TZ_DATE", "TZ_DATETIME", "TZ_TIMESTAMP", "OPTIONAL", "LIST", "DICT", "SET", "TUPLE",
            "STRUCT", "VARIANT", "ENUM", "RESOURCE", "TAGGED", "CALLABLE", "FLOW",
            "DYNUMBER", "JSONDOCUMENT", "TEXT", "BYTES", "DATE32", "DATETIME64", "TIMESTAMP64", "INTERVAL64",
            // Additional types
            "VOID", "UNIT", "EMPTYLIST", "EMPTYDICT", "TZDATE32", "TZDATETIME64", "TZTIMESTAMP64"
        ));
        addFunctions(Arrays.asList(
            // String
            "Length", "Substring", "Find", "RFind", "StartsWith", "EndsWith", "StringReverse", "StringContains",
            "StringReplace", "StringSplitToList", "StringJoinFromList", "StringIsAscii", "StringIsUTF8",
            "StringIsAlNum", "StringIsAlpha", "StringIsDigit", "StringIsLower", "StringIsUpper", "StringIsSpace",
            "StringIsTitle", "StringToLower", "StringToUpper", "StringStrip", "StringLStrip", "StringRStrip",
            "StringRandom", "StringHash", "StringFNV", "StringCrc32", "StringCrc64", "StringLike", "StringMatch",
            "LEN", "ByteAt",
            // Datetime
            "CurrentUtc", "CurrentTz", "Now", "TimestampFromIso", "TimestampFromSeconds", "TimestampToSeconds",
            "DateFromDays", "DateToDays", "DateComponent", "TimeComponent", "DateTimeComponent", "TimezoneId",
            "TimezoneName", "AddTime", "SubTime", "AbsTime",
            "RemoveTimezoneFromTimestamp", "AddTimezone",
            "CurrentUtcDate", "CurrentUtcDatetime", "CurrentUtcTimestamp",
            "CurrentTzDate", "CurrentTzDatetime", "CurrentTzTimestamp",
            // Math
            "Abs", "Acos", "Asin", "Atan", "Atan2", "Cbrt", "Ceil", "Cos", "Cosh", "Erf", "Exp", "Exp2", "Fabs",
            "Floor", "Fmod", "Hypot", "Lgamma", "Log", "Log2", "Log10", "Mod", "Round", "Sin", "Sinh", "Sqrt", "Tan",
            "Tanh", "Trunc",
            // Aggregate
            "COUNT", "COUNT_IF", "SUM", "AVG", "MIN", "MAX", "SOME", "EVERY",
            "SUM_IF", "AVG_IF", "AGG_LIST", "AGG_LIST_DISTINCT",
            "MAX_BY", "MIN_BY", "AGGREGATE_BY", "MULTI_AGGREGATE_BY",
            "TOP_BY", "BOTTOM_BY", "MODE",
            "BOOL_AND", "BOOL_OR", "BOOL_XOR", "BIT_AND", "BIT_OR", "BIT_XOR",
            "VARIANCE", "STDDEV", "CORRELATION", "COVARIANCE",
            "AGGREGATE_LIST", "AGGREGATE_LIST_DISTINCT",
            "TOP", "BOTTOM", "TOPFREQ",
            "HISTOGRAM", "LinearHistogram", "LogHistogram", "LogarithmicHistogram",
            "HyperLogLog", "CountDistinctEstimate", "HLL",
            "Percentile", "Median",
            "CountMinSketch",
            "MAX_OF", "MIN_OF", "GREATEST", "LEAST",
            // Window
            "ROW_NUMBER", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "RANK", "DENSE_RANK",
            "GROUPING", "SessionStart", "SessionState",
            // Conditional
            "Coalesce", "NVL", "NANVL", "IF",
            // Variant/Enum
            "VariantExtract", "VariantItem", "EnumName", "EnumValue",
            // JSON
            "JsonExists", "JsonQuery", "JsonValue",
            // Type inspection and casting
            "EnsureType", "EnsureConvertibleTo", "AssumeStrict", "Likely",
            "Ensure", "Unwrap", "Just", "Nothing",
            "Pickle", "Unpickle",
            "WeakField",
            "FormatType", "ParseType",
            "TestBit", "ClearBit", "SetBit", "FlipBit",
            // Container - List
            "ListCreate", "ListLength", "ListCollect", "ListFlatMap", "ListMap", "ListFilter",
            "ListReverse", "ListSkip", "ListTake", "ListHead", "ListLast", "ListEnumerate",
            "ListMin", "ListMax", "ListSum", "ListAvg",
            "ListFold", "ListFold1", "ListFold1Map", "ListFoldMap",
            "ListAll", "ListAny", "ListZip", "ListZipAll",
            "ListFromRange", "ListReplicate", "ListNotNull", "ListFlatten",
            "ListConcat", "ListSort", "ListSortAsc", "ListSortDesc", "ListHas", "ListHasAny", "ListHasAll",
            "ListUniq", "ListExtend", "ListUnion", "ListIntersection", "ListDifference", "ListSymmetricDifference",
            "ListHasItems", "ListExtendStrict", "ListUnionAll", "ListIndexOf", "ListExtract",
            "ListTakeWhile", "ListSkipWhile", "ListAggregate",
            // Container - Dict
            "DictCreate", "DictLength", "DictKeys", "DictPayloads", "DictItems",
            "DictLookup", "DictContains", "DictAggregate",
            "ToDict", "ToMultiDict", "DictHasItems",
            // Container - Set
            "ToSet", "SetIsDisjoint", "SetIntersection", "SetIncludes",
            "SetUnion", "SetDifference", "SetSymmetricDifference",
            "SetCreate",
            // Struct
            "StaticMap", "StaticZip",
            "TryMember", "ExpandStruct", "AddMember", "RemoveMember", "ForceRemoveMember",
            "ChooseMembers", "RemoveMembers", "ForceRemoveMembers",
            "CombineMembers", "FlattenMembers", "StructMembers",
            "RenameMembers", "ForceRenameMembers", "GatherMembers", "SpreadMembers", "ForceSpreadMembers",
            // Table
            "TableRow", "TableName", "TablePath", "TableRecordIndex",
            // Other
            "Random", "RandomNumber", "RandomUuid", "Uuid", "AsTuple", "AsStruct", "AsList", "AsDict", "AsSet",
            "AsTagged", "Cast", "BitCast", "TypeOf", "IsSame", "FromBytes", "ToBytes",
            "AsListStrict", "AsDictStrict", "AsSetStrict", "AsVariant", "AsEnum",

            // === UDF Functions ===
            // DateTime::
            "DateTime::Format", "DateTime::Parse", "DateTime::MakeDate", "DateTime::MakeDatetime",
            "DateTime::MakeTimestamp", "DateTime::MakeTzDate", "DateTime::MakeTzDatetime",
            "DateTime::MakeTzTimestamp", "DateTime::GetYear", "DateTime::GetDayOfYear",
            "DateTime::GetMonth", "DateTime::GetMonthName", "DateTime::GetWeekOfYear",
            "DateTime::GetWeekOfYearIso8601", "DateTime::GetDayOfMonth", "DateTime::GetDayOfWeek",
            "DateTime::GetDayOfWeekName", "DateTime::GetHour", "DateTime::GetMinute",
            "DateTime::GetSecond", "DateTime::GetMillisecondOfSecond", "DateTime::GetMicrosecondOfSecond",
            "DateTime::GetTimezoneId", "DateTime::GetTimezoneName", "DateTime::Update",
            "DateTime::FromSeconds", "DateTime::FromMilliseconds", "DateTime::FromMicroseconds",
            "DateTime::ToSeconds", "DateTime::ToMilliseconds", "DateTime::ToMicroseconds",
            "DateTime::IntervalFromDays", "DateTime::IntervalFromHours",
            "DateTime::IntervalFromMinutes", "DateTime::IntervalFromSeconds",
            "DateTime::IntervalFromMilliseconds", "DateTime::IntervalFromMicroseconds",
            "DateTime::ToDays", "DateTime::ToHours", "DateTime::ToMinutes",
            "DateTime::StartOfYear", "DateTime::StartOfQuarter", "DateTime::StartOfMonth",
            "DateTime::StartOfWeek", "DateTime::StartOfDay", "DateTime::StartOf",
            "DateTime::TimeOfDay", "DateTime::ShiftYears", "DateTime::ShiftQuarters",
            "DateTime::ShiftMonths",
            // String::
            "String::AsciiToLower", "String::AsciiToUpper", "String::AsciiToTitle",
            "String::Base64Encode", "String::Base64Decode", "String::Base64StrictDecode",
            "String::Base32Encode", "String::Base32Decode", "String::Base32StrictDecode",
            "String::HexEncode", "String::HexDecode",
            "String::EscapeC", "String::UnescapeC",
            "String::EncodeHtml", "String::DecodeHtml",
            "String::CgiEscape", "String::CgiUnescape",
            "String::Strip", "String::Collapse", "String::CollapseText",
            "String::Contains", "String::Find", "String::ReverseFind",
            "String::HasPrefix", "String::HasSuffix", "String::HasPrefixIgnoreCase", "String::HasSuffixIgnoreCase",
            "String::StartsWith", "String::EndsWith", "String::StartsWithIgnoreCase", "String::EndsWithIgnoreCase",
            "String::Substring", "String::AsciiToLower", "String::AsciiToUpper",
            "String::ReplaceAll", "String::ReplaceFirst", "String::ReplaceLast",
            "String::RemoveAll", "String::RemoveFirst", "String::RemoveLast",
            "String::SplitToList", "String::JoinFromList",
            "String::ToByteList", "String::FromByteList",
            "String::Reverse", "String::LevensteinDistance",
            "String::LeftPad", "String::RightPad",
            "String::Hex", "String::SHex", "String::Bin", "String::SBin",
            "String::HumanReadableDuration", "String::HumanReadableQuantity", "String::HumanReadableBytes",
            "String::Prec", "String::IsAscii",
            // Unicode::
            "Unicode::Normalize", "Unicode::NormalizeNFD", "Unicode::NormalizeNFC",
            "Unicode::NormalizeNFKD", "Unicode::NormalizeNFKC",
            "Unicode::Translit", "Unicode::LevensteinDistance",
            "Unicode::Fold", "Unicode::ToLower", "Unicode::ToUpper", "Unicode::ToTitle",
            "Unicode::Strip", "Unicode::IsUtf", "Unicode::IsAscii",
            "Unicode::Substring", "Unicode::Find", "Unicode::ReverseFind",
            "Unicode::Contains", "Unicode::HasPrefix", "Unicode::HasSuffix",
            "Unicode::StartsWith", "Unicode::EndsWith",
            "Unicode::ReplaceAll", "Unicode::ReplaceFirst", "Unicode::ReplaceLast",
            "Unicode::RemoveAll", "Unicode::RemoveFirst", "Unicode::RemoveLast",
            "Unicode::SplitToList", "Unicode::JoinFromList",
            "Unicode::ToCodePointList", "Unicode::FromCodePointList",
            "Unicode::Reverse", "Unicode::GetLength",
            // Url::
            "Url::Normalize", "Url::NormalizeWithDefaultHttpScheme",
            "Url::Encode", "Url::Decode",
            "Url::GetScheme", "Url::GetHost", "Url::GetHostPort", "Url::GetSchemeHost",
            "Url::GetSchemeHostPort", "Url::GetPort",
            "Url::GetTail", "Url::GetPath", "Url::GetFragment", "Url::GetCGIParam",
            "Url::GetDomain", "Url::GetTLD", "Url::GetDomainLevel",
            "Url::GetSignificantDomain", "Url::GetOwner",
            "Url::IsKnownTLD", "Url::IsWellKnownTLD",
            "Url::CutScheme", "Url::CutWWW", "Url::CutWWW2", "Url::CutQueryStringAndFragment",
            "Url::BuildQueryString", "Url::QueryStringToList", "Url::QueryStringToDict",
            "Url::HostNameToPunycode", "Url::ForceHostNameToPunycode",
            "Url::PunycodeToHostName", "Url::ForcePunycodeToHostName",
            "Url::CanBePunycodeHostName",
            // Yson::
            "Yson::Parse", "Yson::ParseJson", "Yson::ParseJsonDecodeUtf8",
            "Yson::From", "Yson::WithAttributes",
            "Yson::Equals", "Yson::GetHash",
            "Yson::IsEntity", "Yson::IsString", "Yson::IsDouble",
            "Yson::IsUint64", "Yson::IsInt64", "Yson::IsBool", "Yson::IsList", "Yson::IsDict",
            "Yson::GetLength",
            "Yson::ConvertTo", "Yson::Contains",
            "Yson::Lookup", "Yson::LookupBool", "Yson::LookupInt64", "Yson::LookupUint64",
            "Yson::LookupDouble", "Yson::LookupString", "Yson::LookupDict", "Yson::LookupList",
            "Yson::YPath", "Yson::YPathBool", "Yson::YPathInt64", "Yson::YPathUint64",
            "Yson::YPathDouble", "Yson::YPathString", "Yson::YPathDict", "Yson::YPathList",
            "Yson::Attributes", "Yson::Serialize", "Yson::SerializeJson",
            "Yson::SerializePretty", "Yson::SerializeText",
            "Yson::Options",
            // Math::
            "Math::Pi", "Math::E", "Math::Eps",
            "Math::Abs", "Math::Acos", "Math::Asin", "Math::Atan", "Math::Atan2",
            "Math::Cbrt", "Math::Ceil", "Math::Cos", "Math::Cosh",
            "Math::Erf", "Math::ErfInv", "Math::ErfcInv",
            "Math::Exp", "Math::Exp2", "Math::Fabs",
            "Math::Floor", "Math::Fmod",
            "Math::FuzzyEquals",
            "Math::Hypot", "Math::IsFinite", "Math::IsInf", "Math::IsNaN",
            "Math::Lgamma", "Math::Log", "Math::Log2", "Math::Log10",
            "Math::Mod", "Math::Pow",
            "Math::Rem", "Math::Round", "Math::Sigmoid",
            "Math::Sin", "Math::Sinh", "Math::Sqrt",
            "Math::Tan", "Math::Tanh", "Math::Tgamma", "Math::Trunc",
            // Digest::
            "Digest::Crc32c", "Digest::Fnv32", "Digest::Fnv64",
            "Digest::MurMurHash", "Digest::MurMurHash2A", "Digest::MurMurHash2A32",
            "Digest::CityHash", "Digest::CityHash128",
            "Digest::NumericHash", "Digest::Md5Hex", "Digest::Md5Raw",
            "Digest::Md5HalfMix", "Digest::Argon2",
            "Digest::Blake2B", "Digest::SipHash",
            "Digest::Sha1", "Digest::Sha256",
            "Digest::IntHash64", "Digest::SuperFastHash",
            "Digest::Xxh3", "Digest::Xxh3_128",
            // Histogram::
            "Histogram::Print", "Histogram::Normalize",
            "Histogram::ToCumulativeDistributionFunction", "Histogram::GetSumAboveBound",
            "Histogram::GetSumBelowBound", "Histogram::GetSumInRange",
            "Histogram::CalcUpperBound", "Histogram::CalcLowerBound",
            "Histogram::CalcUpperBoundSafe", "Histogram::CalcLowerBoundSafe",
            // HyperLogLog::
            "HyperLogLog::Create", "HyperLogLog::AddValue",
            "HyperLogLog::Merge", "HyperLogLog::GetResult", "HyperLogLog::Serialize", "HyperLogLog::Deserialize",
            // Hyperscan::
            "Hyperscan::Grep", "Hyperscan::Match",
            "Hyperscan::BacktrackingGrep", "Hyperscan::BacktrackingMatch",
            "Hyperscan::MultiGrep", "Hyperscan::MultiMatch",
            "Hyperscan::Capture", "Hyperscan::Replace",
            // Ip::
            "Ip::FromString", "Ip::ToString",
            "Ip::IsIPv4", "Ip::IsIPv6", "Ip::IsEmbeddedIPv4",
            "Ip::ConvertToIPv6FromIPv4", "Ip::GetSubnet",
            "Ip::SubnetMatch", "Ip::SubnetFromString", "Ip::SubnetToString",
            // Json::
            "Json::Parse", "Json::From",
            "Json::CompilePath", "Json::SqlQuery", "Json::SqlValue",
            "Json::LookupBool", "Json::LookupInt64", "Json::LookupUint64",
            "Json::LookupDouble", "Json::LookupString",
            "Json::ConvertTo", "Json::Contains",
            "Json::GetField", "Json::GetLength",
            "Json::Serialize", "Json::SerializePretty",
            // Pire::
            "Pire::Grep", "Pire::Match",
            "Pire::MultiGrep", "Pire::MultiMatch",
            "Pire::Capture",
            // Re2::
            "Re2::Grep", "Re2::Match", "Re2::Capture",
            "Re2::FindAndConsume", "Re2::Replace", "Re2::Count",
            "Re2::Options"
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
        if (quoted) {
            return c != '`';
        }
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':';
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

    @NotNull
    @Override
    public List<String> getMatchedKeywords(@NotNull String word) {
        List<String> keywords = new ArrayList<>(super.getMatchedKeywords(word));
        if (dataSource instanceof YDBDataSource ydbDs) {
            YDBAutocompleteClient client = ydbDs.getAutocompleteClient();
            if (client != null) {
                List<AutocompleteEntity> entities = client.fetchEntities(word, 50);
                for (AutocompleteEntity e : entities) {
                    dynamicEntityNames.add(e.getName().toUpperCase());
                    keywords.add(e.getName());
                }
            }
        }
        return keywords;
    }

    @Nullable
    @Override
    public DBPKeywordType getKeywordType(@NotNull String word) {
        DBPKeywordType type = super.getKeywordType(word);
        if (type != null) {
            return type;
        }
        if (dynamicEntityNames.contains(word.toUpperCase())) {
            return DBPKeywordType.OTHER;
        }
        return null;
    }
}
