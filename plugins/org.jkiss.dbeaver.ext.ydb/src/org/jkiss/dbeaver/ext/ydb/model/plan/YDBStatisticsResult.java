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
package org.jkiss.dbeaver.ext.ydb.model.plan;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

public class YDBStatisticsResult {

    private final String queryText;
    private final String planJson;
    private String svgContent;
    private final long totalDurationUs;
    private final long totalCpuTimeUs;

    public YDBStatisticsResult(
        @NotNull String queryText,
        @NotNull String planJson,
        long totalDurationUs,
        long totalCpuTimeUs
    ) {
        this.queryText = queryText;
        this.planJson = planJson;
        this.totalDurationUs = totalDurationUs;
        this.totalCpuTimeUs = totalCpuTimeUs;
    }

    @NotNull
    public String getQueryText() {
        return queryText;
    }

    @NotNull
    public String getPlanJson() {
        return planJson;
    }

    @Nullable
    public String getSvgContent() {
        return svgContent;
    }

    public void setSvgContent(@Nullable String svgContent) {
        this.svgContent = svgContent;
    }

    public long getTotalDurationUs() {
        return totalDurationUs;
    }

    public long getTotalCpuTimeUs() {
        return totalCpuTimeUs;
    }
}
