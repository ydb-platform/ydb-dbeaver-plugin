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

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.browser.BrowserFunction;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBStatisticsResult;
import org.jkiss.dbeaver.runtime.DBWorkbench;

import java.util.Base64;

/**
 * Dialog that displays the SVG rendering of a YDB query execution plan with statistics.
 * Uses an embedded SWT Browser widget to render the SVG content.
 */
public class YDBStatisticsSvgDialog extends Dialog {

    private static final Log log = Log.getLog(YDBStatisticsSvgDialog.class);
    private static final int EXPORT_PNG_ID = IDialogConstants.CLIENT_ID + 1;

    private final YDBStatisticsResult result;
    private Browser browser;

    public YDBStatisticsSvgDialog(Shell parentShell, YDBStatisticsResult result) {
        super(parentShell);
        this.result = result;
        setShellStyle(getShellStyle() | SWT.RESIZE | SWT.MAX);
    }

    @Override
    protected void configureShell(Shell newShell) {
        super.configureShell(newShell);
        newShell.setText("YDB Query Statistics");
        newShell.setSize(1200, 800);
    }

    @Override
    protected Control createDialogArea(Composite parent) {
        Composite container = (Composite) super.createDialogArea(parent);
        container.setLayout(new GridLayout(1, false));

        // Stats summary
        String summary = String.format("Total duration: %.2f ms  |  Total CPU time: %.2f ms",
            result.getTotalDurationUs() / 1000.0,
            result.getTotalCpuTimeUs() / 1000.0);
        Label summaryLabel = new Label(container, SWT.NONE);
        summaryLabel.setText(summary);
        summaryLabel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        // SVG content in Browser
        String svgContent = result.getSvgContent();
        if (svgContent != null && !svgContent.isEmpty()) {
            browser = new Browser(container, SWT.NONE);
            browser.setLayoutData(new GridData(GridData.FILL_BOTH));

            // Register callback for receiving PNG data from JavaScript
            new BrowserFunction(browser, "savePngData") {
                @Override
                public Object function(Object[] arguments) {
                    if (arguments.length > 0 && arguments[0] instanceof String) {
                        savePngToFile((String) arguments[0]);
                    }
                    return null;
                }
            };

            String html = buildHtmlPage(svgContent);
            browser.setText(html);
        } else {
            // Fallback: show raw plan JSON
            Label fallbackLabel = new Label(container, SWT.NONE);
            fallbackLabel.setText("SVG rendering unavailable. Raw plan JSON:");
            fallbackLabel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

            org.eclipse.swt.widgets.Text jsonText = new org.eclipse.swt.widgets.Text(
                container, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.READ_ONLY);
            jsonText.setText(result.getPlanJson());
            jsonText.setLayoutData(new GridData(GridData.FILL_BOTH));
        }

        return container;
    }

    @Override
    protected void createButtonsForButtonBar(Composite parent) {
        if (result.getSvgContent() != null && !result.getSvgContent().isEmpty()) {
            createButton(parent, EXPORT_PNG_ID, "Export PNG", false);
        }
        createButton(parent, IDialogConstants.OK_ID, IDialogConstants.CLOSE_LABEL, true);
    }

    @Override
    protected void buttonPressed(int buttonId) {
        if (buttonId == EXPORT_PNG_ID) {
            exportPng();
        } else {
            super.buttonPressed(buttonId);
        }
    }

    private void exportPng() {
        if (browser == null) {
            return;
        }
        browser.execute("window.__exportPng();");
    }

    private void savePngToFile(String base64Data) {
        if (base64Data == null || base64Data.startsWith("ERROR")) {
            log.error("PNG export failed in browser: " + base64Data);
            DBWorkbench.getPlatformUI().showError("Export PNG", "Failed to render PNG from SVG: " + base64Data);
            return;
        }

        FileDialog fileDialog = new FileDialog(getShell(), SWT.SAVE);
        fileDialog.setFilterExtensions(new String[]{"*.png"});
        fileDialog.setFilterNames(new String[]{"PNG Image (*.png)"});
        fileDialog.setFileName("ydb_statistics.png");
        fileDialog.setOverwrite(true);

        String filePath = fileDialog.open();
        if (filePath == null) {
            return;
        }

        try {
            byte[] pngBytes = Base64.getDecoder().decode(base64Data);
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath)) {
                fos.write(pngBytes);
            }
        } catch (Exception e) {
            log.error("Failed to save PNG file", e);
            DBWorkbench.getPlatformUI().showError("Export PNG", "Failed to save PNG: " + e.getMessage());
        }
    }

    private static String buildHtmlPage(String svgContent) {
        return "<!DOCTYPE html>\n"
            + "<html><head><meta charset='utf-8'><style>\n"
            + "* { margin: 0; padding: 0; box-sizing: border-box; }\n"
            + "html, body { width: 100%; height: 100%; overflow: hidden; background: #f5f5f5; font-family: -apple-system, sans-serif; }\n"
            // toolbar
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
            // viewport
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
            // toolbar HTML with inline SVG icons
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
            // viewport
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
            // toolbar buttons
            + "  document.getElementById('btn-zin').onclick = function() { setScale(scale * 1.25); };\n"
            + "  document.getElementById('btn-zout').onclick = function() { setScale(scale / 1.25); };\n"
            + "  document.getElementById('btn-reset').onclick = function() {\n"
            + "    scale = 1; ct.style.transform = 'scale(1)';\n"
            + "    lbl.textContent = '100%'; vp.scrollLeft = 0; vp.scrollTop = 0;\n"
            + "  };\n"
            + "\n"
            // wheel zoom
            + "  vp.addEventListener('wheel', function(e) {\n"
            + "    if (!e.ctrlKey && !e.metaKey) return;\n"
            + "    e.preventDefault();\n"
            + "    var rect = vp.getBoundingClientRect();\n"
            + "    var cx = e.clientX - rect.left, cy = e.clientY - rect.top;\n"
            + "    var d = e.deltaY > 0 ? 1/1.1 : 1.1;\n"
            + "    setScale(scale * d, cx, cy);\n"
            + "  }, {passive: false});\n"
            + "\n"
            // pinch-to-zoom
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
            // drag pan (left button)
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
            + "\n"
            // PNG export function
            + "  window.__exportPng = function() {\n"
            + "    var svg = document.querySelector('#content svg');\n"
            + "    if (!svg) { window.savePngData('ERROR:no_svg_element'); return; }\n"
            + "    var clone = svg.cloneNode(true);\n"
            + "    var w = 0, h = 0;\n"
            + "    try { w = svg.width.baseVal.value; h = svg.height.baseVal.value; } catch(e) {}\n"
            + "    if (!w || !h) { var r = svg.getBoundingClientRect(); w = r.width; h = r.height; }\n"
            + "    if (!w || !h) { try { var bb = svg.getBBox(); w = bb.width + bb.x; h = bb.height + bb.y; } catch(e) {} }\n"
            + "    if (!w || !h) { window.savePngData('ERROR:cannot_determine_svg_size'); return; }\n"
            + "    clone.setAttribute('width', w);\n"
            + "    clone.setAttribute('height', h);\n"
            + "    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');\n"
            + "    clone.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');\n"
            + "    var data = new XMLSerializer().serializeToString(clone);\n"
            + "    var blob = new Blob([data], {type: 'image/svg+xml;charset=utf-8'});\n"
            + "    var url = URL.createObjectURL(blob);\n"
            + "    var img = new Image();\n"
            + "    img.onload = function() {\n"
            + "      try {\n"
            + "        var c = document.createElement('canvas');\n"
            + "        c.width = w * 2; c.height = h * 2;\n"
            + "        var ctx = c.getContext('2d');\n"
            + "        ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, c.width, c.height);\n"
            + "        ctx.drawImage(img, 0, 0, c.width, c.height);\n"
            + "        var b64 = c.toDataURL('image/png').split(',')[1];\n"
            + "        window.savePngData(b64);\n"
            + "      } catch(e) {\n"
            + "        window.savePngData('ERROR:canvas_' + e.message);\n"
            + "      } finally {\n"
            + "        URL.revokeObjectURL(url);\n"
            + "      }\n"
            + "    };\n"
            + "    img.onerror = function() { URL.revokeObjectURL(url); window.savePngData('ERROR:img_load_failed'); };\n"
            + "    img.src = url;\n"
            + "  };\n"
            + "})();\n"
            + "</script></body></html>";
    }
}
