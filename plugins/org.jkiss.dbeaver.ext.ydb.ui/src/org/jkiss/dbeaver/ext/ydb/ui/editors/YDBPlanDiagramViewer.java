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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.events.MouseTrackAdapter;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ScrollBar;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlanNode;

import java.util.*;
import java.util.function.Consumer;

/**
 * Custom SWT Canvas that draws a YDB execution plan as a horizontal graphical diagram
 * with compact boxes connected by lines. The tree grows left-to-right.
 * <p>
 * Hover shows a minimal tooltip. Click or keyboard arrows select a node and notify the listener.
 */
public class YDBPlanDiagramViewer extends Canvas {

    private static final int NODE_PADDING_X = 12;
    private static final int NODE_PADDING_Y = 8;
    private static final int NODE_H_GAP = 40;
    private static final int NODE_V_GAP = 20;
    private static final int ARC_SIZE = 12;
    private static final int MARGIN = 30;
    private static final int ROOT_CIRCLE_RADIUS = 16;
    private static final int LINE_FROM_CIRCLE = 20;
    private static final int MAX_LABEL_WIDTH = 200;

    private java.util.List<YDBPlanNode> rootNodes = Collections.emptyList();
    private final Map<YDBPlanNode, Rectangle> nodeBounds = new LinkedHashMap<>();
    private final Map<YDBPlanNode, Integer> nodeNumbers = new LinkedHashMap<>();
    private int totalWidth;
    private int totalHeight;

    private Font boldFont;
    private Font smallFont;

    private int originX;
    private int originY;

    private YDBPlanNode hoveredNode;
    private YDBPlanNode selectedNode;
    private Consumer<YDBPlanNode> nodeSelectionListener;

    public YDBPlanDiagramViewer(@NotNull Composite parent) {
        super(parent, SWT.H_SCROLL | SWT.V_SCROLL | SWT.DOUBLE_BUFFERED);

        setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));

        addPaintListener(this::paint);
        addDisposeListener(e -> disposeFonts());

        ScrollBar hBar = getHorizontalBar();
        ScrollBar vBar = getVerticalBar();
        if (hBar != null) {
            hBar.addListener(SWT.Selection, e -> {
                originX = -hBar.getSelection();
                redraw();
            });
        }
        if (vBar != null) {
            vBar.addListener(SWT.Selection, e -> {
                originY = -vBar.getSelection();
                redraw();
            });
        }
        addListener(SWT.Resize, e -> updateScrollBars());

        // Tooltip on hover
        addMouseMoveListener(new MouseMoveListener() {
            @Override
            public void mouseMove(MouseEvent e) {
                YDBPlanNode node = hitTest(e.x, e.y);
                if (node != hoveredNode) {
                    hoveredNode = node;
                    if (node != null) {
                        setToolTipText(buildTooltipText(node));
                    } else {
                        setToolTipText(null);
                    }
                }
            }
        });
        addMouseTrackListener(new MouseTrackAdapter() {
            @Override
            public void mouseExit(MouseEvent e) {
                hoveredNode = null;
                setToolTipText(null);
            }
        });

        // Click selects a node
        addMouseListener(new org.eclipse.swt.events.MouseAdapter() {
            @Override
            public void mouseDown(MouseEvent e) {
                setFocus();
                YDBPlanNode node = hitTest(e.x, e.y);
                selectNode(node);
            }
        });

        // Keyboard navigation
        addKeyListener(new org.eclipse.swt.events.KeyAdapter() {
            @Override
            public void keyPressed(org.eclipse.swt.events.KeyEvent e) {
                handleKeyNavigation(e.keyCode);
            }
        });
    }

    public void setNodeSelectionListener(@Nullable Consumer<YDBPlanNode> listener) {
        this.nodeSelectionListener = listener;
    }

    public void setInput(@NotNull java.util.List<YDBPlanNode> nodes) {
        this.rootNodes = nodes;
        this.selectedNode = null;
        layoutDiagram();
        originX = 0;
        originY = 0;
        updateScrollBars();
        redraw();
    }

    private void selectNode(@Nullable YDBPlanNode node) {
        if (node != selectedNode) {
            selectedNode = node;
            if (node != null) {
                ensureNodeVisible(node);
            }
            redraw();
            if (nodeSelectionListener != null) {
                nodeSelectionListener.accept(node);
            }
        }
    }

    private void ensureNodeVisible(@NotNull YDBPlanNode node) {
        Rectangle bounds = nodeBounds.get(node);
        if (bounds == null) {
            return;
        }
        Rectangle client = getClientArea();
        int nodeScreenX = bounds.x + originX;
        int nodeScreenY = bounds.y + originY;

        boolean changed = false;
        if (nodeScreenX < 0) {
            originX = -bounds.x;
            changed = true;
        } else if (nodeScreenX + bounds.width > client.width) {
            originX = client.width - bounds.x - bounds.width;
            changed = true;
        }
        if (nodeScreenY < 0) {
            originY = -bounds.y;
            changed = true;
        } else if (nodeScreenY + bounds.height > client.height) {
            originY = client.height - bounds.y - bounds.height;
            changed = true;
        }
        if (changed) {
            updateScrollBars();
        }
    }

    // --- Keyboard navigation ---

    private void handleKeyNavigation(int keyCode) {
        if (rootNodes.isEmpty()) {
            return;
        }
        if (selectedNode == null) {
            selectNode(rootNodes.get(0));
            return;
        }

        YDBPlanNode target = null;
        switch (keyCode) {
            case SWT.ARROW_LEFT:
                // Go to parent
                target = getParentNode(selectedNode);
                break;
            case SWT.ARROW_RIGHT:
                // Go to first child
                Collection<YDBPlanNode> children = selectedNode.getNested();
                if (!children.isEmpty()) {
                    target = children.iterator().next();
                }
                break;
            case SWT.ARROW_UP:
                // Go to previous sibling
                target = getSibling(selectedNode, -1);
                break;
            case SWT.ARROW_DOWN:
                // Go to next sibling
                target = getSibling(selectedNode, 1);
                break;
            default:
                return;
        }
        if (target != null) {
            selectNode(target);
        }
    }

    @Nullable
    private YDBPlanNode getParentNode(@NotNull YDBPlanNode node) {
        Object parent = node.getParent();
        if (parent instanceof YDBPlanNode) {
            return (YDBPlanNode) parent;
        }
        return null;
    }

    @Nullable
    private YDBPlanNode getSibling(@NotNull YDBPlanNode node, int direction) {
        YDBPlanNode parentNode = getParentNode(node);
        List<YDBPlanNode> siblings;
        if (parentNode != null) {
            siblings = new ArrayList<>(parentNode.getNested());
        } else {
            siblings = rootNodes;
        }
        int idx = siblings.indexOf(node);
        if (idx < 0) {
            return null;
        }
        int newIdx = idx + direction;
        if (newIdx >= 0 && newIdx < siblings.size()) {
            return siblings.get(newIdx);
        }
        return null;
    }

    // --- Hit testing ---

    @Nullable
    private YDBPlanNode hitTest(int mouseX, int mouseY) {
        int mx = mouseX - originX;
        int my = mouseY - originY;
        for (Map.Entry<YDBPlanNode, Rectangle> entry : nodeBounds.entrySet()) {
            if (entry.getValue().contains(mx, my)) {
                return entry.getKey();
            }
        }
        return null;
    }

    // --- Tooltip (minimal) ---

    @NotNull
    private String buildTooltipText(@NotNull YDBPlanNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getNodeType());
        if (node.getNodeName() != null && !node.getNodeName().isEmpty()) {
            sb.append("\nTables: ").append(node.getNodeName());
        }
        if (node.getOperators() != null && !node.getOperators().isEmpty()) {
            sb.append("\nOperators: ").append(node.getOperators());
        }
        return sb.toString();
    }

    // --- Font management ---

    private void ensureFonts() {
        if (boldFont == null) {
            Font current = getFont();
            FontData[] fd = current.getFontData();
            for (FontData f : fd) {
                f.setStyle(SWT.BOLD);
            }
            boldFont = new Font(getDisplay(), fd);

            FontData[] sd = current.getFontData();
            for (FontData f : sd) {
                f.setHeight(f.getHeight() - 1);
            }
            smallFont = new Font(getDisplay(), sd);
        }
    }

    private void disposeFonts() {
        if (boldFont != null) {
            boldFont.dispose();
            boldFont = null;
        }
        if (smallFont != null) {
            smallFont.dispose();
            smallFont = null;
        }
    }

    // --- Layout (horizontal: left-to-right) ---

    private void layoutDiagram() {
        ensureFonts();
        nodeBounds.clear();
        nodeNumbers.clear();

        GC gc = new GC(this);
        try {
            int[] counter = {0};
            for (YDBPlanNode root : rootNodes) {
                measureNode(gc, root, counter);
            }
            int x = MARGIN + ROOT_CIRCLE_RADIUS * 2 + LINE_FROM_CIRCLE;
            for (YDBPlanNode root : rootNodes) {
                int subtreeHeight = getSubtreeHeight(root);
                int y = MARGIN + subtreeHeight / 2 - nodeBounds.get(root).height / 2;
                positionNode(root, x, y);
            }
            totalWidth = 0;
            totalHeight = 0;
            for (Rectangle r : nodeBounds.values()) {
                totalWidth = Math.max(totalWidth, r.x + r.width);
                totalHeight = Math.max(totalHeight, r.y + r.height);
            }
            totalWidth += MARGIN;
            totalHeight += MARGIN;
        } finally {
            gc.dispose();
        }
    }

    private void measureNode(GC gc, YDBPlanNode node, int[] counter) {
        counter[0]++;
        nodeNumbers.put(node, counter[0]);

        gc.setFont(boldFont);
        Point typeSize = gc.textExtent(node.getNodeType());

        int textWidth = typeSize.x;
        int textHeight = typeSize.y;

        gc.setFont(smallFont);

        String subtitle = getNodeSubtitle(node);
        if (subtitle != null) {
            Point subSize = gc.textExtent(subtitle);
            int subWidth = Math.min(subSize.x, MAX_LABEL_WIDTH);
            textWidth = Math.max(textWidth, subWidth);
            textHeight += subSize.y + 2;
        }

        String numStr = "#" + nodeNumbers.get(node);
        Point numSize = gc.textExtent(numStr);
        textWidth = Math.max(textWidth, typeSize.x + 16 + numSize.x);

        int nodeWidth = textWidth + NODE_PADDING_X * 2;
        int nodeHeight = textHeight + NODE_PADDING_Y * 2;

        nodeBounds.put(node, new Rectangle(0, 0, nodeWidth, nodeHeight));

        for (YDBPlanNode child : node.getNested()) {
            measureNode(gc, child, counter);
        }
    }

    @Nullable
    private static String getNodeSubtitle(@NotNull YDBPlanNode node) {
        if (node.getNodeName() != null && !node.getNodeName().isEmpty()) {
            return "Tables: " + node.getNodeName();
        }
        return null;
    }

    private int getSubtreeHeight(YDBPlanNode node) {
        Collection<YDBPlanNode> children = node.getNested();
        if (children.isEmpty()) {
            return nodeBounds.get(node).height;
        }
        int childrenTotalHeight = 0;
        for (YDBPlanNode child : children) {
            if (childrenTotalHeight > 0) {
                childrenTotalHeight += NODE_V_GAP;
            }
            childrenTotalHeight += getSubtreeHeight(child);
        }
        return Math.max(nodeBounds.get(node).height, childrenTotalHeight);
    }

    private void positionNode(YDBPlanNode node, int x, int y) {
        Rectangle bounds = nodeBounds.get(node);
        bounds.x = x;
        bounds.y = y;

        Collection<YDBPlanNode> children = node.getNested();
        if (children.isEmpty()) {
            return;
        }

        int childX = x + bounds.width + NODE_H_GAP;
        int nodeCenterY = y + bounds.height / 2;
        int childrenTotalHeight = 0;
        for (YDBPlanNode child : children) {
            if (childrenTotalHeight > 0) {
                childrenTotalHeight += NODE_V_GAP;
            }
            childrenTotalHeight += getSubtreeHeight(child);
        }

        int childY = nodeCenterY - childrenTotalHeight / 2;
        for (YDBPlanNode child : children) {
            int childSubtreeHeight = getSubtreeHeight(child);
            Rectangle childBounds = nodeBounds.get(child);
            int cy = childY + childSubtreeHeight / 2 - childBounds.height / 2;
            positionNode(child, childX, cy);
            childY += childSubtreeHeight + NODE_V_GAP;
        }
    }

    // --- Painting ---

    private void paint(PaintEvent e) {
        GC gc = e.gc;
        gc.setAntialias(SWT.ON);
        gc.setTextAntialias(SWT.ON);

        ensureFonts();

        gc.setBackground(getBackground());
        gc.fillRectangle(getClientArea());

        if (rootNodes.isEmpty()) {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_GRAY));
            gc.drawText("No execution plan available", 20, 20, true);
            return;
        }

        // Root circle (on the left)
        for (YDBPlanNode root : rootNodes) {
            Rectangle bounds = nodeBounds.get(root);
            if (bounds == null) continue;
            int cy = originY + bounds.y + bounds.height / 2;
            int circleX = originX + MARGIN;

            gc.setBackground(getDisplay().getSystemColor(SWT.COLOR_WHITE));
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_GRAY));
            gc.setLineWidth(2);
            gc.drawOval(circleX, cy - ROOT_CIRCLE_RADIUS,
                ROOT_CIRCLE_RADIUS * 2, ROOT_CIRCLE_RADIUS * 2);

            gc.setLineWidth(1);
            gc.drawLine(circleX + ROOT_CIRCLE_RADIUS * 2, cy, originX + bounds.x, cy);
        }

        // Edges (horizontal: parent right side -> child left side)
        gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_GRAY));
        gc.setLineWidth(1);
        for (Map.Entry<YDBPlanNode, Rectangle> entry : nodeBounds.entrySet()) {
            YDBPlanNode node = entry.getKey();
            Rectangle parentBounds = entry.getValue();
            for (YDBPlanNode child : node.getNested()) {
                Rectangle childBounds = nodeBounds.get(child);
                if (childBounds == null) continue;
                int px = originX + parentBounds.x + parentBounds.width;
                int py = originY + parentBounds.y + parentBounds.height / 2;
                int chx = originX + childBounds.x;
                int chy = originY + childBounds.y + childBounds.height / 2;
                gc.drawLine(px, py, chx, chy);
            }
        }

        // Nodes
        for (Map.Entry<YDBPlanNode, Rectangle> entry : nodeBounds.entrySet()) {
            drawNode(gc, entry.getKey(), entry.getValue());
        }
    }

    private void drawNode(GC gc, YDBPlanNode node, Rectangle bounds) {
        int x = originX + bounds.x;
        int y = originY + bounds.y;
        boolean isSelected = node == selectedNode;

        // Background
        if (isSelected) {
            gc.setBackground(getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION));
        } else {
            gc.setBackground(getDisplay().getSystemColor(SWT.COLOR_WHITE));
        }
        gc.fillRoundRectangle(x, y, bounds.width, bounds.height, ARC_SIZE, ARC_SIZE);

        // Border
        if (isSelected) {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION_TEXT));
        } else {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_WIDGET_NORMAL_SHADOW));
        }
        gc.setLineWidth(isSelected ? 2 : 1);
        gc.drawRoundRectangle(x, y, bounds.width, bounds.height, ARC_SIZE, ARC_SIZE);

        // Node type (bold)
        gc.setFont(boldFont);
        if (isSelected) {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION_TEXT));
        } else {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_BLACK));
        }
        int textX = x + NODE_PADDING_X;
        int textY = y + NODE_PADDING_Y;
        gc.drawText(node.getNodeType(), textX, textY, true);

        // Node number (top-right)
        gc.setFont(smallFont);
        if (isSelected) {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION_TEXT));
        } else {
            gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_GRAY));
        }
        Integer num = nodeNumbers.get(node);
        if (num != null) {
            String numStr = "#" + num;
            Point numSize = gc.textExtent(numStr);
            gc.drawText(numStr, x + bounds.width - NODE_PADDING_X - numSize.x,
                y + NODE_PADDING_Y, true);
        }

        // Subtitle (tables)
        String subtitle = getNodeSubtitle(node);
        if (subtitle != null) {
            gc.setFont(smallFont);
            if (isSelected) {
                gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION_TEXT));
            } else {
                gc.setForeground(getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
            }
            gc.setFont(boldFont);
            Point typeSize = gc.textExtent(node.getNodeType());
            gc.setFont(smallFont);
            textY += typeSize.y + 2;
            String drawn = ellipsize(gc, subtitle, bounds.width - NODE_PADDING_X * 2);
            gc.drawText(drawn, textX, textY, true);
        }
    }

    @NotNull
    private static String ellipsize(@NotNull GC gc, @NotNull String text, int maxWidth) {
        Point extent = gc.textExtent(text);
        if (extent.x <= maxWidth) {
            return text;
        }
        String ellipsis = "...";
        Point ellipsisSize = gc.textExtent(ellipsis);
        int available = maxWidth - ellipsisSize.x;
        if (available <= 0) {
            return ellipsis;
        }
        for (int i = text.length() - 1; i > 0; i--) {
            if (gc.textExtent(text.substring(0, i)).x <= available) {
                return text.substring(0, i) + ellipsis;
            }
        }
        return ellipsis;
    }

    // --- Scrollbars ---

    private void updateScrollBars() {
        Rectangle client = getClientArea();
        ScrollBar hBar = getHorizontalBar();
        ScrollBar vBar = getVerticalBar();
        if (hBar != null) {
            hBar.setMaximum(totalWidth);
            hBar.setThumb(client.width);
            hBar.setPageIncrement(client.width);
            hBar.setSelection(-originX);
        }
        if (vBar != null) {
            vBar.setMaximum(totalHeight);
            vBar.setThumb(client.height);
            vBar.setPageIncrement(client.height);
            vBar.setSelection(-originY);
        }
    }
}
