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

import org.jkiss.dbeaver.model.DBPKeywordType;
import org.jkiss.dbeaver.model.sql.SQLDialect;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class YDBSQLDialectTest {

    private YDBSQLDialect dialect;

    @Before
    public void setUp() {
        dialect = new YDBSQLDialect();
        dialect.initDriverSettings(null, null, null);
    }

    @Test
    public void testDialectName() {
        assertEquals("YDB", dialect.getDialectName());
    }

    @Test
    public void testIdentifierQuoteString() {
        assertEquals("`", dialect.getIdentifierQuoteStrings()[0][0]);
    }

    @Test
    public void testSupportsUnquotedMixedCase() {
        assertFalse(dialect.supportsUnquotedMixedCase());
    }

    @Test
    public void testKeywords() {
        assertEquals(DBPKeywordType.KEYWORD, dialect.getKeywordType("SELECT"));
        assertEquals(DBPKeywordType.KEYWORD, dialect.getKeywordType("CREATE"));
        assertEquals(DBPKeywordType.KEYWORD, dialect.getKeywordType("TABLE"));
        assertEquals(DBPKeywordType.KEYWORD, dialect.getKeywordType("STREAM"));
        assertEquals(DBPKeywordType.KEYWORD, dialect.getKeywordType("TOPIC"));
    }

    @Test
    public void testNewKeywordsPresent() {
        assertEquals("AUTOMAP should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("AUTOMAP"));
        assertEquals("COLLATE should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("COLLATE"));
        assertEquals("DEFINE should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("DEFINE"));
        assertEquals("DO should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("DO"));
        assertEquals("ON should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("ON"));
        assertEquals("REPEATABLE should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("REPEATABLE"));
        assertEquals("SAMPLE should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("SAMPLE"));
        assertEquals("SET should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("SET"));
        assertEquals("USING should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("USING"));
        assertEquals("VIEW should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("VIEW"));
        assertEquals("WITH should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("WITH"));
        assertEquals("WINDOW should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("WINDOW"));
        assertEquals("TRUE should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("TRUE"));
        assertEquals("FALSE should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("FALSE"));
        assertEquals("PRESORT should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("PRESORT"));
        assertEquals("RESULT should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("RESULT"));
        assertEquals("SEMI should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("SEMI"));
        assertEquals("KEY should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("KEY"));
        assertEquals("INNER should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("INNER"));
        assertEquals("JOIN should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("JOIN"));
        assertEquals("NATURAL should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("NATURAL"));
        assertEquals("OUTER should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("OUTER"));
        assertEquals("OVER should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("OVER"));
        assertEquals("CROSS should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("CROSS"));
        assertEquals("EXCLUSION should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("EXCLUSION"));
        assertEquals("ISNULL should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("ISNULL"));
        assertEquals("NOTNULL should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("NOTNULL"));
        assertEquals("LEFT should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("LEFT"));
        assertEquals("RIGHT should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("RIGHT"));
    }

    @Test
    public void testRemovedKeywordsAbsent() {
        assertNull("COMPRESSION should not be present", dialect.getKeywordType("COMPRESSION"));
        assertNull("DYNAMICLINEAR should not be present", dialect.getKeywordType("DYNAMICLINEAR"));
        assertNull("LINEAR should not be present", dialect.getKeywordType("LINEAR"));
        assertNull("TRUNCATE should not be present", dialect.getKeywordType("TRUNCATE"));
    }

    @Test
    public void testDataTypes() {
        Collection<String> dataTypes = dialect.getDataTypes(null);
        assertTrue(dataTypes.contains("STRING"));
        assertTrue(dataTypes.contains("INT32"));
        assertTrue(dataTypes.contains("TIMESTAMP"));
        assertTrue(dataTypes.contains("LIST"));
    }

    @Test
    public void testNewDataTypes() {
        Collection<String> dataTypes = dialect.getDataTypes(null);
        assertTrue("DYNUMBER should be present", dataTypes.contains("DYNUMBER"));
        assertTrue("JSONDOCUMENT should be present", dataTypes.contains("JSONDOCUMENT"));
        assertTrue("TEXT should be present", dataTypes.contains("TEXT"));
        assertTrue("BYTES should be present", dataTypes.contains("BYTES"));
        assertTrue("DATE32 should be present", dataTypes.contains("DATE32"));
        assertTrue("DATETIME64 should be present", dataTypes.contains("DATETIME64"));
        assertTrue("TIMESTAMP64 should be present", dataTypes.contains("TIMESTAMP64"));
        assertTrue("INTERVAL64 should be present", dataTypes.contains("INTERVAL64"));
    }

    @Test
    public void testFunctions() {
        // getFunctions() returns values in original case; use getKeywordType for case-insensitive check
        assertEquals(DBPKeywordType.FUNCTION, dialect.getKeywordType("COUNT"));
        assertEquals(DBPKeywordType.FUNCTION, dialect.getKeywordType("SUM"));
        assertEquals(DBPKeywordType.FUNCTION, dialect.getKeywordType("SUBSTRING"));
    }

    @Test
    public void testNewFunctions() {
        // Conditional (Coalesce may be registered as KEYWORD by standard SQL dialect, check it's recognized)
        assertNotNull("Coalesce should be recognized", dialect.getKeywordType("COALESCE"));
        assertEquals("NVL should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("NVL"));
        // List functions
        assertEquals("ListCreate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTCREATE"));
        assertEquals("ListLength should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTLENGTH"));
        assertEquals("ListMap should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTMAP"));
        assertEquals("ListFilter should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTFILTER"));
        // Dict functions
        assertEquals("DictCreate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DICTCREATE"));
        assertEquals("DictKeys should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DICTKEYS"));
        // Set functions
        assertEquals("ToSet should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TOSET"));
        assertEquals("SetUnion should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("SETUNION"));
        // Table functions
        assertEquals("TableRow should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TABLEROW"));
        assertEquals("TableName should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TABLENAME"));
        // Aggregate
        assertEquals("HISTOGRAM should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("HISTOGRAM"));
        assertEquals("HyperLogLog should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("HYPERLOGLOG"));
        assertEquals("VARIANCE should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("VARIANCE"));
        assertEquals("STDDEV should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STDDEV"));
        // Type
        assertEquals("Unwrap should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("UNWRAP"));
        assertEquals("Just should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("JUST"));
        assertEquals("Nothing should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("NOTHING"));

        // New functions
        assertEquals("VariantExtract should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("VARIANTEXTRACT"));
        assertEquals("VariantItem should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("VARIANTITEM"));
        assertEquals("EnumName should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ENUMNAME"));
        assertEquals("EnumValue should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ENUMVALUE"));
        assertEquals("JsonExists should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("JSONEXISTS"));
        assertEquals("JsonQuery should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("JSONQUERY"));
        assertEquals("JsonValue should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("JSONVALUE"));
    }

    @Test
    public void testStringQuoteStrings() {
        String[][] quotes = dialect.getStringQuoteStrings();
        assertEquals(2, quotes.length);
        assertEquals("'", quotes[0][0]);
        assertEquals("'", quotes[0][1]);
        assertEquals("\"", quotes[1][0]);
        assertEquals("\"", quotes[1][1]);
    }

    @Test
    public void testParametersPrefixes() {
        String[] prefixes = dialect.getParametersPrefixes();
        assertEquals(1, prefixes.length);
        assertEquals("$", prefixes[0]);
    }

    @Test
    public void testBlockBoundStrings() {
        String[][] bounds = dialect.getBlockBoundStrings();
        assertNotNull(bounds);
        assertEquals(1, bounds.length);
        assertEquals("BEGIN", bounds[0][0]);
        assertEquals("END", bounds[0][1]);
    }

    @Test
    public void testBlockHeaderStrings() {
        String[] headers = dialect.getBlockHeaderStrings();
        assertNotNull(headers);
        List<String> headerList = Arrays.asList(headers);
        assertTrue(headerList.contains("DEFINE"));
        assertTrue(headerList.contains("DO"));
    }

    @Test
    public void testSupportsSubqueries() {
        assertTrue(dialect.supportsSubqueries());
    }

    @Test
    public void testSupportsAliasInSelect() {
        assertTrue(dialect.supportsAliasInSelect());
    }

    @Test
    public void testSupportsAliasInConditions() {
        assertFalse(dialect.supportsAliasInConditions());
    }

    @Test
    public void testSupportsTableDropCascade() {
        assertTrue(dialect.supportsTableDropCascade());
    }

    @Test
    public void testSupportsOrderByIndex() {
        assertFalse(dialect.supportsOrderByIndex());
    }

    @Test
    public void testSupportsInsertAllDefaultValuesStatement() {
        assertTrue(dialect.supportsInsertAllDefaultValuesStatement());
    }

    @Test
    public void testSupportsCommentQuery() {
        assertTrue(dialect.supportsCommentQuery());
    }

    @Test
    public void testSupportsNestedComments() {
        assertTrue(dialect.supportsNestedComments());
    }

    @Test
    public void testGetTestSQL() {
        assertEquals("SELECT 1", dialect.getTestSQL());
    }

    @Test
    public void testGetExecuteKeywords() {
        String[] execKeywords = dialect.getExecuteKeywords();
        List<String> execList = Arrays.asList(execKeywords);
        assertTrue("DO should be in execute keywords", execList.contains("DO"));
        assertTrue("EVALUATE should be in execute keywords", execList.contains("EVALUATE"));
    }

    @Test
    public void testGetDDLKeywords() {
        String[] ddl = dialect.getDDLKeywords();
        List<String> ddlList = Arrays.asList(ddl);
        assertTrue(ddlList.contains("CREATE"));
        assertTrue(ddlList.contains("ALTER"));
        assertTrue(ddlList.contains("DROP"));
        assertTrue(ddlList.contains("UPSERT"));
    }

    @Test
    public void testGetDMLKeywords() {
        String[] dml = dialect.getDMLKeywords();
        List<String> dmlList = Arrays.asList(dml);
        assertTrue(dmlList.contains("INSERT"));
        assertTrue(dmlList.contains("DELETE"));
        assertTrue(dmlList.contains("UPDATE"));
        assertTrue(dmlList.contains("UPSERT"));
        assertTrue(dmlList.contains("REPLACE"));
    }

    @Test
    public void testTransactionKeywords() {
        String[] commit = dialect.getTransactionCommitKeywords();
        assertNotNull(commit);
        assertEquals("COMMIT", commit[0]);

        String[] rollback = dialect.getTransactionRollbackKeywords();
        assertNotNull(rollback);
        assertEquals("ROLLBACK", rollback[0]);
    }

    @Test
    public void testDefaultMultiValueInsertMode() {
        assertEquals(SQLDialect.MultiValueInsertMode.GROUP_ROWS, dialect.getDefaultMultiValueInsertMode());
    }

    @Test
    public void testStringEscapeCharacter() {
        assertEquals('\\', dialect.getStringEscapeCharacter());
    }

    @Test
    public void testValidIdentifierStart() {
        assertTrue(dialect.validIdentifierStart('a'));
        assertTrue(dialect.validIdentifierStart('Z'));
        assertTrue(dialect.validIdentifierStart('_'));
        assertFalse(dialect.validIdentifierStart('0'));
        assertFalse(dialect.validIdentifierStart('$'));
    }

    @Test
    public void testValidIdentifierPart() {
        assertTrue(dialect.validIdentifierPart('a', false));
        assertTrue(dialect.validIdentifierPart('Z', false));
        assertTrue(dialect.validIdentifierPart('_', false));
        assertTrue(dialect.validIdentifierPart('5', false));
        assertTrue("Colon should be valid for UDF syntax", dialect.validIdentifierPart(':', false));
        assertFalse(dialect.validIdentifierPart('$', false));
        assertFalse(dialect.validIdentifierPart('-', false));
    }

    @Test
    public void testSingleLineComments() {
        String[] comments = dialect.getSingleLineComments();
        assertEquals(1, comments.length);
        assertEquals("--", comments[0]);
    }

    @Test
    public void testAdditionalDataTypes() {
        Collection<String> dataTypes = dialect.getDataTypes(null);
        assertTrue("VOID should be present", dataTypes.contains("VOID"));
        assertTrue("UNIT should be present", dataTypes.contains("UNIT"));
        assertTrue("EMPTYLIST should be present", dataTypes.contains("EMPTYLIST"));
        assertTrue("EMPTYDICT should be present", dataTypes.contains("EMPTYDICT"));
        assertTrue("TZDATE32 should be present", dataTypes.contains("TZDATE32"));
        assertTrue("TZDATETIME64 should be present", dataTypes.contains("TZDATETIME64"));
        assertTrue("TZTIMESTAMP64 should be present", dataTypes.contains("TZTIMESTAMP64"));
    }

    @Test
    public void testAggregateFunctions() {
        assertEquals("SUM_IF should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("SUM_IF"));
        assertEquals("AVG_IF should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("AVG_IF"));
        assertEquals("AGG_LIST should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("AGG_LIST"));
        assertEquals("AGG_LIST_DISTINCT should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("AGG_LIST_DISTINCT"));
        assertEquals("MAX_BY should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("MAX_BY"));
        assertEquals("MIN_BY should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("MIN_BY"));
        assertEquals("AGGREGATE_BY should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("AGGREGATE_BY"));
        assertEquals("MODE should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("MODE"));
        assertEquals("BOOL_AND should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("BOOL_AND"));
        assertEquals("BOOL_OR should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("BOOL_OR"));
        assertEquals("BIT_AND should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("BIT_AND"));
        assertEquals("BIT_OR should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("BIT_OR"));
    }

    @Test
    public void testWindowFunctions() {
        // ROW_NUMBER may be registered as KEYWORD by parent dialect; just verify it's recognized
        assertNotNull("ROW_NUMBER should be recognized", dialect.getKeywordType("ROW_NUMBER"));
        assertEquals("LAG should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LAG"));
        assertEquals("LEAD should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LEAD"));
        assertEquals("FIRST_VALUE should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("FIRST_VALUE"));
        assertEquals("LAST_VALUE should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LAST_VALUE"));
        // RANK and DENSE_RANK may be registered as KEYWORD by parent dialect; just verify recognized
        assertNotNull("RANK should be recognized", dialect.getKeywordType("RANK"));
        assertNotNull("DENSE_RANK should be recognized", dialect.getKeywordType("DENSE_RANK"));
    }

    @Test
    public void testContainerFunctions() {
        assertEquals("ListHasItems should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTHASITEMS"));
        assertEquals("ListExtendStrict should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTEXTENDSTRICT"));
        assertEquals("ListAggregate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LISTAGGREGATE"));
        assertEquals("ToDict should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TODICT"));
        assertEquals("ToMultiDict should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TOMULTIDICT"));
        assertEquals("SetCreate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("SETCREATE"));
        assertEquals("DictHasItems should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DICTHASITEMS"));
    }

    @Test
    public void testStructFunctions() {
        assertEquals("TryMember should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TRYMEMBER"));
        assertEquals("ExpandStruct should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("EXPANDSTRUCT"));
        assertEquals("AddMember should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ADDMEMBER"));
        assertEquals("RemoveMember should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("REMOVEMEMBER"));
        assertEquals("ChooseMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("CHOOSEMEMBERS"));
        assertEquals("CombineMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("COMBINEMEMBERS"));
        assertEquals("FlattenMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("FLATTENMEMBERS"));
        assertEquals("StructMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STRUCTMEMBERS"));
        assertEquals("RenameMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("RENAMEMEMBERS"));
        assertEquals("GatherMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("GATHERMEMBERS"));
        assertEquals("SpreadMembers should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("SPREADMEMBERS"));
    }

    @Test
    public void testMiscFunctions() {
        assertEquals("LEN should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("LEN"));
        assertEquals("CurrentUtcDate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("CURRENTUTCDATE"));
        assertEquals("CurrentUtcDatetime should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("CURRENTUTCDATETIME"));
        assertEquals("CurrentUtcTimestamp should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("CURRENTUTCTIMESTAMP"));
        assertEquals("CurrentTzDate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("CURRENTTZDATE"));
        assertEquals("FormatType should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("FORMATTYPE"));
        assertEquals("ParseType should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("PARSETYPE"));
        assertEquals("TestBit should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("TESTBIT"));
        assertEquals("SetBit should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("SETBIT"));
        assertEquals("AsListStrict should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ASLISTSTRICT"));
        assertEquals("AsDictStrict should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ASDICTSTRICT"));
        assertEquals("AsVariant should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ASVARIANT"));
        assertEquals("AsEnum should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("ASENUM"));
    }

    @Test
    public void testUdfDateTimeFunctions() {
        assertEquals("DateTime::Format should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DATETIME::FORMAT"));
        assertEquals("DateTime::Parse should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DATETIME::PARSE"));
        assertEquals("DateTime::MakeDate should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DATETIME::MAKEDATE"));
        assertEquals("DateTime::GetYear should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DATETIME::GETYEAR"));
        assertEquals("DateTime::GetMonth should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DATETIME::GETMONTH"));
        assertEquals("DateTime::StartOfYear should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DATETIME::STARTOFYEAR"));
    }

    @Test
    public void testUdfStringFunctions() {
        assertEquals("String::AsciiToLower should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STRING::ASCIITOLOWER"));
        assertEquals("String::Base64Encode should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STRING::BASE64ENCODE"));
        assertEquals("String::HexEncode should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STRING::HEXENCODE"));
        assertEquals("String::Contains should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STRING::CONTAINS"));
        assertEquals("String::SplitToList should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("STRING::SPLITTOLIST"));
    }

    @Test
    public void testUdfUnicodeFunctions() {
        assertEquals("Unicode::ToLower should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("UNICODE::TOLOWER"));
        assertEquals("Unicode::GetLength should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("UNICODE::GETLENGTH"));
    }

    @Test
    public void testUdfUrlFunctions() {
        assertEquals("Url::Normalize should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("URL::NORMALIZE"));
        assertEquals("Url::GetHost should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("URL::GETHOST"));
        assertEquals("Url::Encode should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("URL::ENCODE"));
    }

    @Test
    public void testUdfYsonFunctions() {
        assertEquals("Yson::Parse should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("YSON::PARSE"));
        assertEquals("Yson::Serialize should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("YSON::SERIALIZE"));
        assertEquals("Yson::Lookup should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("YSON::LOOKUP"));
    }

    @Test
    public void testUdfMathFunctions() {
        assertEquals("Math::Pi should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("MATH::PI"));
        assertEquals("Math::Abs should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("MATH::ABS"));
        assertEquals("Math::Pow should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("MATH::POW"));
    }

    @Test
    public void testUdfDigestFunctions() {
        assertEquals("Digest::Md5Hex should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DIGEST::MD5HEX"));
        assertEquals("Digest::Sha256 should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("DIGEST::SHA256"));
    }

    @Test
    public void testUdfHyperscanFunctions() {
        assertEquals("Hyperscan::Grep should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("HYPERSCAN::GREP"));
        assertEquals("Hyperscan::Match should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("HYPERSCAN::MATCH"));
    }

    @Test
    public void testUdfRe2Functions() {
        assertEquals("Re2::Grep should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("RE2::GREP"));
        assertEquals("Re2::Match should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("RE2::MATCH"));
        assertEquals("Re2::Replace should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("RE2::REPLACE"));
    }

    @Test
    public void testUdfJsonFunctions() {
        assertEquals("Json::Parse should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("JSON::PARSE"));
        assertEquals("Json::Serialize should be a function", DBPKeywordType.FUNCTION, dialect.getKeywordType("JSON::SERIALIZE"));
    }

    @Test
    public void testPragmaKeywords() {
        assertEquals("TablePathPrefix should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("TABLEPATHPREFIX"));
        assertEquals("Warning should be a keyword", DBPKeywordType.KEYWORD, dialect.getKeywordType("WARNING"));
    }

    @Test
    public void testGetMatchedKeywordsWithoutDataSource() {
        // Without a dataSource, getMatchedKeywords should still return standard keywords
        List<String> matched = dialect.getMatchedKeywords("SELEC");
        assertTrue("Should match SELECT", matched.stream().anyMatch(k -> k.equalsIgnoreCase("SELECT")));
    }

    @Test
    public void testGetKeywordTypeForDynamicEntity() {
        // Dynamic entities not yet added should return null
        assertNull(dialect.getKeywordType("my_custom_table_12345"));
    }
}
