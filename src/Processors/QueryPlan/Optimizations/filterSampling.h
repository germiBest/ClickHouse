#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <base/types.h>
#include <optional>

namespace DB
{

/// Estimate filter selectivity by sampling evenly-spaced granules across all parts
/// and ranges, evaluating the filter on each, and returning the median selectivity.
/// The median provides robustness against outlier granules.
/// Deterministic positioning ensures reproducible query plans.
///
/// Handles both a parent FilterStep expression (`filter_dag`/`filter_column_name`) and a PREWHERE
/// condition on the ReadFromMergeTree step. When both are present, both are evaluated and their
/// combined (AND) selectivity is returned.
///
/// If `analyzed_result` is provided, it is reused instead of calling `selectRangesToRead` again.
std::optional<Float64> estimateFilterSelectivity(
    const ReadFromMergeTree & read_step,
    const ActionsDAG * filter_dag = nullptr,
    const String * filter_column_name = nullptr,
    const ReadFromMergeTree::AnalysisResultPtr & analyzed_result = nullptr);

}
