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
package org.jkiss.dbeaver.ext.ydb.ui.editors;

import org.eclipse.core.expressions.PropertyTester;
import org.jkiss.dbeaver.ext.ydb.model.YDBStreamingQuery;

/**
 * Property tester for YDB streaming query status.
 * Used in plugin.xml to conditionally show Start/Stop menu items.
 */
public class YDBStreamingQueryPropertyTester extends PropertyTester {

    private static final String PROP_IS_RUNNING = "isRunning";

    @Override
    public boolean test(Object receiver, String property, Object[] args, Object expectedValue) {
        if (receiver instanceof YDBStreamingQuery) {
            YDBStreamingQuery query = (YDBStreamingQuery) receiver;
            if (PROP_IS_RUNNING.equals(property)) {
                boolean isRunning = "RUNNING".equalsIgnoreCase(query.getStatus());
                if (expectedValue instanceof Boolean) {
                    return isRunning == (Boolean) expectedValue;
                }
                return isRunning == Boolean.parseBoolean(String.valueOf(expectedValue));
            }
        }
        return false;
    }
}
