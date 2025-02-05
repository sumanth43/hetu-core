/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.plan.TableWriterNode.UpdateTarget;
import io.prestosql.sql.tree.Expression;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class UpdateNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final UpdateTarget target;
    private final Symbol rowId;
    private final List<Symbol> columnValueAndRowIdSymbols;
    private final List<Symbol> outputs;
    private final Map<String, Expression> updateColumnExpression;

    @JsonCreator
    public UpdateNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") UpdateTarget target,
            @JsonProperty("rowId") Symbol rowId,
            @JsonProperty("columnValueAndRowIdSymbols") List<Symbol> columnValueAndRowIdSymbols,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("updateColumnExpression") Map<String, Expression> updateColumnExpression)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.columnValueAndRowIdSymbols = ImmutableList.copyOf(requireNonNull(columnValueAndRowIdSymbols, "columnValueAndRowIdSymbols is null"));
        this.outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.updateColumnExpression = requireNonNull(updateColumnExpression, "outputs is null");
        int symbolsSize = columnValueAndRowIdSymbols.size();
        int columnsSize = target.getUpdatedColumns().size();
        checkArgument(symbolsSize == columnsSize + 1, "The symbol count %s must be one greater than updated columns count %s", symbolsSize, columnsSize);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public UpdateTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public Symbol getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public List<Symbol> getColumnValueAndRowIdSymbols()
    {
        return columnValueAndRowIdSymbols;
    }

    @JsonProperty("outputs")
    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty
    public Map<String, Expression> getUpdateColumnExpression()
    {
        return updateColumnExpression;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUpdate(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new UpdateNode(getId(), Iterables.getOnlyElement(newChildren), target, rowId, columnValueAndRowIdSymbols, outputs, updateColumnExpression);
    }
}
