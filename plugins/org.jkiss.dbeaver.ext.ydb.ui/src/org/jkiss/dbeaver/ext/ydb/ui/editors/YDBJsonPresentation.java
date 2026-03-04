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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.model.data.DBDContent;
import org.jkiss.dbeaver.model.data.DBDDisplayFormat;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.controls.resultset.AbstractPresentation;
import org.jkiss.dbeaver.ui.controls.resultset.IResultSetController;
import org.jkiss.dbeaver.ui.controls.resultset.ResultSetCopySettings;
import org.jkiss.dbeaver.ui.controls.resultset.ResultSetModel;
import org.jkiss.dbeaver.ui.controls.resultset.ResultSetRow;

import java.util.Collections;
import java.util.Map;

/**
 * JSON result set presentation.
 * Shows the current cell value as formatted JSON.
 */
public class YDBJsonPresentation extends AbstractPresentation {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().serializeNulls().disableHtmlEscaping().create();

    private StyledText text;
    private DBDAttributeBinding curAttribute;

    @Override
    public void createPresentation(@NotNull IResultSetController controller, @NotNull Composite parent) {
        super.createPresentation(controller, parent);

        text = new StyledText(parent, SWT.READ_ONLY | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
        text.setFont(UIUtils.getMonospaceFont());
        text.setEditable(false);

        registerContextMenu();
        activateTextKeyBindings(controller, text);
        trackPresentationControl();
    }

    @Override
    public Control getControl() {
        return text;
    }

    @Override
    public void refreshData(boolean refreshMetadata, boolean append, boolean keepState) {
        showCellValue();
    }

    private void showCellValue() {
        ResultSetModel model = controller.getModel();
        ResultSetRow currentRow = controller.getCurrentRow();

        if (currentRow == null || curAttribute == null) {
            text.setText("");
            return;
        }

        Object value = model.getCellValue(curAttribute, currentRow);
        JsonElement json = toJsonElement(curAttribute, value);
        text.setText(GSON.toJson(json));
    }

    private JsonElement toJsonElement(DBDAttributeBinding attr, Object value) {
        if (value == null) {
            return JsonNull.INSTANCE;
        }

        if (value instanceof DBDContent content) {
            Object rawValue = content.getRawValue();
            if (rawValue instanceof String strValue) {
                return tryParseJson(strValue);
            }
            return rawValue == null ? JsonNull.INSTANCE : new JsonPrimitive(rawValue.toString());
        }

        if (value instanceof Number num) {
            return new JsonPrimitive(num);
        }
        if (value instanceof Boolean bool) {
            return new JsonPrimitive(bool);
        }

        String strValue = attr.getValueHandler().getValueDisplayString(attr, value, DBDDisplayFormat.EDIT);
        return tryParseJson(strValue);
    }

    private JsonElement tryParseJson(String str) {
        if (str == null) {
            return JsonNull.INSTANCE;
        }
        String trimmed = str.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            try {
                return JsonParser.parseString(trimmed);
            } catch (JsonSyntaxException e) {
                // Not valid JSON, treat as string
            }
        }
        return new JsonPrimitive(str);
    }

    @Override
    public void formatData(boolean refreshData) {
    }

    @Override
    public void clearMetaData() {
        text.setText("");
    }

    @Override
    public void updateValueView() {
        showCellValue();
    }

    @Override
    public void changeMode(boolean recordMode) {
    }

    @Nullable
    @Override
    public DBDAttributeBinding getCurrentAttribute() {
        return curAttribute;
    }

    @Override
    public void setCurrentAttribute(@NotNull DBDAttributeBinding attribute) {
        this.curAttribute = attribute;
        showCellValue();
    }

    @NotNull
    @Override
    public Map<Transfer, Object> copySelection(ResultSetCopySettings settings) {
        String selectedText = text.getSelectionText();
        if (selectedText == null || selectedText.isEmpty()) {
            selectedText = text.getText();
        }
        return Collections.singletonMap(TextTransfer.getInstance(), selectedText);
    }

    @Override
    public void fillMenu(@NotNull IMenuManager menu) {
    }

    @Override
    public void scrollToRow(@NotNull RowPosition position) {
        super.scrollToRow(position);
        showCellValue();
    }

    @Override
    public void dispose() {
    }
}
