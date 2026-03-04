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

import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.YDBDataSource;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlan2SvgClient;
import org.jkiss.dbeaver.ext.ydb.ui.YDBConnectionPage;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.sql.SQLQuery;
import org.jkiss.utils.CommonUtils;

/**
 * JFace Viewer that displays a YDB execution plan as SVG using plan2svg.
 * This viewer fetches the plan JSON from the plan nodes, sends it to the
 * YDB viewer endpoint for SVG conversion, and displays the result.
 */
public class YDBStatisticsSvgJFaceViewer extends Viewer {

    private static final Log log = Log.getLog(YDBStatisticsSvgJFaceViewer.class);

    private final Composite composite;
    private Browser browser;
    private Label statusLabel;

    public YDBStatisticsSvgJFaceViewer(Composite parent) {
        composite = new Composite(parent, SWT.NONE);
        composite.setLayout(new GridLayout(1, false));
        composite.setLayoutData(new GridData(GridData.FILL_BOTH));

        statusLabel = new Label(composite, SWT.NONE);
        statusLabel.setText("No plan loaded");
        statusLabel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        browser = new Browser(composite, SWT.NONE);
        browser.setLayoutData(new GridData(GridData.FILL_BOTH));
    }

    public void showPlan(SQLQuery query, DBCPlan plan) {
        statusLabel.setText("Plan for: " + query.getText());

        // Extract plan JSON from the plan feature if available
        Object planJsonObj = plan.getPlanFeature("planJson");
        if (planJsonObj instanceof String) {
            String planJson = (String) planJsonObj;
            renderPlanJson(planJson, query);
            return;
        }

        // Fallback: display message that SVG view requires statistics mode
        String html = "<!DOCTYPE html><html><body style='font-family: sans-serif; padding: 20px;'>"
            + "<h3>SVG Statistics View</h3>"
            + "<p>This view shows query execution statistics as SVG.</p>"
            + "<p>Use the <b>Statistics</b> toolbar button to execute a query with profiling "
            + "and view the detailed plan SVG.</p>"
            + "<p>The standard EXPLAIN plan does not include runtime statistics needed for SVG rendering.</p>"
            + "</body></html>";
        browser.setText(html);
    }

    private void renderPlanJson(String planJson, SQLQuery query) {
        // Try to get viewer URL from the datasource
        try {
            if (query.getDataSource() instanceof YDBDataSource) {
                YDBDataSource ds = (YDBDataSource) query.getDataSource();
                DBPConnectionConfiguration config = ds.getContainer().getConnectionConfiguration();

                String viewerUrl = config.getProviderProperty(YDBConnectionPage.PROP_MONITORING_URL);
                if (CommonUtils.isEmpty(viewerUrl)) {
                    viewerUrl = YDBPlan2SvgClient.buildViewerUrl(config.getHostName(), 8765);
                }

                String authToken = null;
                String authType = config.getProviderProperty(YDBConnectionPage.PROP_AUTH_TYPE);
                if ("token".equals(authType)) {
                    authToken = config.getProviderProperty(YDBConnectionPage.PROP_TOKEN);
                }

                String svg = YDBPlan2SvgClient.convertToSvg(viewerUrl, planJson, authToken);

                String html = buildHtmlPage(svg);
                browser.setText(html);
                return;
            }
        } catch (Exception e) {
            log.warn("Failed to convert plan to SVG", e);
        }

        // Fallback: show raw JSON
        String escaped = planJson.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        String html = "<!DOCTYPE html><html><body><pre>" + escaped + "</pre></body></html>";
        browser.setText(html);
    }

    @Override
    public Control getControl() {
        return composite;
    }

    @Override
    public Object getInput() {
        return null;
    }

    @Override
    public ISelection getSelection() {
        return null;
    }

    @Override
    public void refresh() {
    }

    @Override
    public void setInput(Object input) {
    }

    @Override
    public void setSelection(ISelection selection, boolean reveal) {
    }

    private static String buildHtmlPage(String svgContent) {
        return "<!DOCTYPE html>\n"
            + "<html><head><meta charset='utf-8'><style>\n"
            + "* { margin: 0; padding: 0; box-sizing: border-box; }\n"
            + "html, body { width: 100%; height: 100%; overflow: hidden; background: #f5f5f5; font-family: -apple-system, sans-serif; }\n"
            + "#toolbar {\n"
            + "  display: flex; align-items: center; gap: 4px;\n"
            + "  padding: 4px 8px; background: #e8e8e8; border-bottom: 1px solid #ccc;\n"
            + "  user-select: none; -webkit-user-select: none;\n"
            + "}\n"
            + "#toolbar button {\n"
            + "  display: flex; align-items: center; justify-content: center;\n"
            + "  width: 28px; height: 28px; border: 1px solid #aaa; border-radius: 4px;\n"
            + "  background: #fff; cursor: pointer; font-size: 16px; padding: 0;\n"
            + "}\n"
            + "#toolbar button:hover { background: #dbeafe; border-color: #7ba4db; }\n"
            + "#toolbar button:active { background: #bfdbfe; }\n"
            + "#zoom-label { font-size: 12px; min-width: 44px; text-align: center; color: #333; }\n"
            + "#toolbar .sep { width: 1px; height: 20px; background: #bbb; margin: 0 4px; }\n"
            + "#viewport {\n"
            + "  width: 100%; height: calc(100% - 37px); overflow: auto;\n"
            + "  background: #fff; cursor: grab;\n"
            + "  touch-action: none;\n"
            + "}\n"
            + "#viewport.dragging { cursor: grabbing; }\n"
            + "#content {\n"
            + "  transform-origin: 0 0; display: inline-block;\n"
            + "  padding: 8px;\n"
            + "}\n"
            + "#content svg { display: block; }\n"
            + "</style></head><body>\n"
            + "<div id='toolbar'>\n"
            + "  <button id='btn-zin' title='Zoom In'>"
            + "<svg width='16' height='16' viewBox='0 0 16 16'>"
            + "<circle cx='7' cy='7' r='5.5' fill='none' stroke='#333' stroke-width='1.5'/>"
            + "<line x1='11' y1='11' x2='14.5' y2='14.5' stroke='#333' stroke-width='1.5' stroke-linecap='round'/>"
            + "<line x1='4.5' y1='7' x2='9.5' y2='7' stroke='#333' stroke-width='1.5' stroke-linecap='round'/>"
            + "<line x1='7' y1='4.5' x2='7' y2='9.5' stroke='#333' stroke-width='1.5' stroke-linecap='round'/>"
            + "</svg></button>\n"
            + "  <button id='btn-zout' title='Zoom Out'>"
            + "<svg width='16' height='16' viewBox='0 0 16 16'>"
            + "<circle cx='7' cy='7' r='5.5' fill='none' stroke='#333' stroke-width='1.5'/>"
            + "<line x1='11' y1='11' x2='14.5' y2='14.5' stroke='#333' stroke-width='1.5' stroke-linecap='round'/>"
            + "<line x1='4.5' y1='7' x2='9.5' y2='7' stroke='#333' stroke-width='1.5' stroke-linecap='round'/>"
            + "</svg></button>\n"
            + "  <button id='btn-reset' title='Reset Zoom'>"
            + "<svg width='16' height='16' viewBox='0 0 16 16'>"
            + "<rect x='2' y='2' width='12' height='12' rx='2' fill='none' stroke='#333' stroke-width='1.5'/>"
            + "<polyline points='5,8 8,5 11,8' fill='none' stroke='#333' stroke-width='1.3' stroke-linecap='round' stroke-linejoin='round'/>"
            + "<polyline points='5,11 8,8 11,11' fill='none' stroke='#333' stroke-width='1.3' stroke-linecap='round' stroke-linejoin='round'/>"
            + "</svg></button>\n"
            + "  <div class='sep'></div>\n"
            + "  <span id='zoom-label'>100%</span>\n"
            + "</div>\n"
            + "<div id='viewport'><div id='content'>\n"
            + svgContent
            + "\n</div></div>\n"
            + "<script>\n"
            + "(function() {\n"
            + "  var vp = document.getElementById('viewport');\n"
            + "  var ct = document.getElementById('content');\n"
            + "  var lbl = document.getElementById('zoom-label');\n"
            + "  var scale = 1;\n"
            + "  var MIN = 0.1, MAX = 20;\n"
            + "\n"
            + "  function setScale(s, cx, cy) {\n"
            + "    s = Math.max(MIN, Math.min(MAX, s));\n"
            + "    if (cx !== undefined && cy !== undefined) {\n"
            + "      var scrollX = vp.scrollLeft, scrollY = vp.scrollTop;\n"
            + "      var ptX = (scrollX + cx) / scale;\n"
            + "      var ptY = (scrollY + cy) / scale;\n"
            + "      scale = s;\n"
            + "      ct.style.transform = 'scale(' + scale + ')';\n"
            + "      lbl.textContent = Math.round(scale * 100) + '%';\n"
            + "      vp.scrollLeft = ptX * scale - cx;\n"
            + "      vp.scrollTop = ptY * scale - cy;\n"
            + "    } else {\n"
            + "      var cxv = vp.clientWidth / 2, cyv = vp.clientHeight / 2;\n"
            + "      setScale(s, cxv, cyv);\n"
            + "    }\n"
            + "  }\n"
            + "\n"
            + "  document.getElementById('btn-zin').onclick = function() { setScale(scale * 1.25); };\n"
            + "  document.getElementById('btn-zout').onclick = function() { setScale(scale / 1.25); };\n"
            + "  document.getElementById('btn-reset').onclick = function() {\n"
            + "    scale = 1; ct.style.transform = 'scale(1)';\n"
            + "    lbl.textContent = '100%'; vp.scrollLeft = 0; vp.scrollTop = 0;\n"
            + "  };\n"
            + "\n"
            + "  vp.addEventListener('wheel', function(e) {\n"
            + "    if (!e.ctrlKey && !e.metaKey) return;\n"
            + "    e.preventDefault();\n"
            + "    var rect = vp.getBoundingClientRect();\n"
            + "    var cx = e.clientX - rect.left, cy = e.clientY - rect.top;\n"
            + "    var d = e.deltaY > 0 ? 1/1.1 : 1.1;\n"
            + "    setScale(scale * d, cx, cy);\n"
            + "  }, {passive: false});\n"
            + "\n"
            + "  var pinchDist = 0, pinchScale = 1;\n"
            + "  function touchDist(e) {\n"
            + "    var dx = e.touches[0].clientX - e.touches[1].clientX;\n"
            + "    var dy = e.touches[0].clientY - e.touches[1].clientY;\n"
            + "    return Math.sqrt(dx*dx + dy*dy);\n"
            + "  }\n"
            + "  function touchCenter(e) {\n"
            + "    var rect = vp.getBoundingClientRect();\n"
            + "    return {\n"
            + "      x: (e.touches[0].clientX + e.touches[1].clientX) / 2 - rect.left,\n"
            + "      y: (e.touches[0].clientY + e.touches[1].clientY) / 2 - rect.top\n"
            + "    };\n"
            + "  }\n"
            + "  vp.addEventListener('touchstart', function(e) {\n"
            + "    if (e.touches.length === 2) {\n"
            + "      e.preventDefault();\n"
            + "      pinchDist = touchDist(e); pinchScale = scale;\n"
            + "    }\n"
            + "  }, {passive: false});\n"
            + "  vp.addEventListener('touchmove', function(e) {\n"
            + "    if (e.touches.length === 2) {\n"
            + "      e.preventDefault();\n"
            + "      var d = touchDist(e);\n"
            + "      var c = touchCenter(e);\n"
            + "      setScale(pinchScale * (d / pinchDist), c.x, c.y);\n"
            + "    }\n"
            + "  }, {passive: false});\n"
            + "\n"
            + "  var dragging = false, lx, ly;\n"
            + "  vp.addEventListener('mousedown', function(e) {\n"
            + "    if (e.button === 0) {\n"
            + "      dragging = true; lx = e.clientX; ly = e.clientY;\n"
            + "      vp.classList.add('dragging'); e.preventDefault();\n"
            + "    }\n"
            + "  });\n"
            + "  window.addEventListener('mousemove', function(e) {\n"
            + "    if (!dragging) return;\n"
            + "    vp.scrollLeft -= (e.clientX - lx); vp.scrollTop -= (e.clientY - ly);\n"
            + "    lx = e.clientX; ly = e.clientY;\n"
            + "  });\n"
            + "  window.addEventListener('mouseup', function() {\n"
            + "    dragging = false; vp.classList.remove('dragging');\n"
            + "  });\n"
            + "})();\n"
            + "</script></body></html>";
    }
}
