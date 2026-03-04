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

import org.jkiss.dbeaver.ext.ydb.model.autocomplete.AutocompleteEntity;
import org.jkiss.dbeaver.ext.ydb.model.autocomplete.YDBAutocompleteClient;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class YDBAutocompleteClientTest {

    @Test
    public void testParseEntitiesWithValidResponse() {
        String json = "{\"Result\":{\"Entities\":["
            + "{\"Name\":\"my_table\",\"Type\":\"table\",\"Parent\":\"/local\"},"
            + "{\"Name\":\"my_view\",\"Type\":\"view\"}"
            + "]}}";
        List<AutocompleteEntity> entities = YDBAutocompleteClient.parseEntities(json);
        assertEquals(2, entities.size());
        assertEquals("my_table", entities.get(0).getName());
        assertEquals("table", entities.get(0).getType());
        assertEquals("/local", entities.get(0).getParent());
        assertEquals("my_view", entities.get(1).getName());
        assertEquals("view", entities.get(1).getType());
        assertNull(entities.get(1).getParent());
    }

    @Test
    public void testParseEntitiesWithEmptyResult() {
        String json = "{\"Result\":{\"Entities\":[]}}";
        List<AutocompleteEntity> entities = YDBAutocompleteClient.parseEntities(json);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void testParseEntitiesWithNoResultField() {
        String json = "{\"Status\":\"OK\"}";
        List<AutocompleteEntity> entities = YDBAutocompleteClient.parseEntities(json);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void testParseEntitiesWithNoEntitiesField() {
        String json = "{\"Result\":{}}";
        List<AutocompleteEntity> entities = YDBAutocompleteClient.parseEntities(json);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void testParseEntitiesSkipsEmptyNames() {
        String json = "{\"Result\":{\"Entities\":["
            + "{\"Name\":\"\",\"Type\":\"table\"},"
            + "{\"Name\":\"valid_table\",\"Type\":\"table\"}"
            + "]}}";
        List<AutocompleteEntity> entities = YDBAutocompleteClient.parseEntities(json);
        assertEquals(1, entities.size());
        assertEquals("valid_table", entities.get(0).getName());
    }

    @Test
    public void testParseEntitiesWithColumnType() {
        String json = "{\"Result\":{\"Entities\":["
            + "{\"Name\":\"id\",\"Type\":\"column\",\"Parent\":\"my_table\"},"
            + "{\"Name\":\"name\",\"Type\":\"column\",\"Parent\":\"my_table\"}"
            + "]}}";
        List<AutocompleteEntity> entities = YDBAutocompleteClient.parseEntities(json);
        assertEquals(2, entities.size());
        assertEquals("id", entities.get(0).getName());
        assertEquals("column", entities.get(0).getType());
        assertEquals("my_table", entities.get(0).getParent());
    }

    @Test
    public void testCircuitBreakerInitiallyAvailable() {
        YDBAutocompleteClient client = new YDBAutocompleteClient(
            "http://localhost:8765", "/local", null);
        assertTrue(client.isAvailable());
    }

    @Test
    public void testFetchEntitiesWithUnreachableServer() {
        // Use a non-routable address to trigger circuit breaker
        YDBAutocompleteClient client = new YDBAutocompleteClient(
            "http://192.0.2.1:1", "/local", null);
        List<AutocompleteEntity> entities = client.fetchEntities("test", 10);
        assertTrue("Should return empty list on failure", entities.isEmpty());
        assertFalse("Circuit breaker should trip after failure", client.isAvailable());
    }

    @Test
    public void testAutocompleteEntityGetters() {
        AutocompleteEntity entity = new AutocompleteEntity("test_name", "table", "/parent");
        assertEquals("test_name", entity.getName());
        assertEquals("table", entity.getType());
        assertEquals("/parent", entity.getParent());
    }

    @Test
    public void testAutocompleteEntityNullFields() {
        AutocompleteEntity entity = new AutocompleteEntity("name", null, null);
        assertEquals("name", entity.getName());
        assertNull(entity.getType());
        assertNull(entity.getParent());
    }
}
