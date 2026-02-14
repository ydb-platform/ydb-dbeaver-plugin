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
        assertFalse(dialect.validIdentifierPart('$', false));
        assertFalse(dialect.validIdentifierPart('-', false));
    }

    @Test
    public void testSingleLineComments() {
        String[] comments = dialect.getSingleLineComments();
        assertEquals(1, comments.length);
        assertEquals("--", comments[0]);
    }
}
