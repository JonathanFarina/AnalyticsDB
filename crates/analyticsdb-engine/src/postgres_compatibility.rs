use std::collections::HashSet;

use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{LogicalPlan, Projection};
use datafusion::optimizer::analyzer::AnalyzerRule;

#[derive(Debug)]
pub struct DuplicateColumnAlerter;

impl Default for DuplicateColumnAlerter {
    fn default() -> Self {
        Self::new()
    }
}

impl DuplicateColumnAlerter {
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerRule for DuplicateColumnAlerter {
    fn name(&self) -> &str {
        "duplicate_column_alerter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        self.analyze_internal(plan)
    }
}

impl DuplicateColumnAlerter {
    fn analyze_internal(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let new_inputs = plan
            .inputs()
            .into_iter()
            .map(|input| self.analyze_internal(input.clone()))
            .collect::<Result<Vec<_>>>()?;

        let plan = plan.with_new_exprs(plan.expressions(), new_inputs)?;

        if let LogicalPlan::Projection(projection) = plan {
            let mut seen_names = HashSet::new();
            let mut new_exprs = Vec::new();
            let mut changed = false;

            for expr in &projection.expr {
                let name = expr.to_string();
                if seen_names.contains(&name) {
                    let mut i = 1;
                    let mut new_name = format!("{}_{}", name, i);
                    while seen_names.contains(&new_name) {
                        i += 1;
                        new_name = format!("{}_{}", name, i);
                    }
                    new_exprs.push(expr.clone().alias(new_name.clone()));
                    seen_names.insert(new_name);
                    changed = true;
                } else {
                    seen_names.insert(name);
                    new_exprs.push(expr.clone());
                }
            }

            if changed {
                return Ok(LogicalPlan::Projection(Projection::try_new(
                    new_exprs,
                    projection.input,
                )?));
            } else {
                return Ok(LogicalPlan::Projection(projection));
            }
        }

        Ok(plan)
    }
}
