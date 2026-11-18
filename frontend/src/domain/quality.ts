/** 质量工作台领域模型：采集质量谱系、隔离记录与汇总的纯前端类型与判定逻辑。 */

export type QualityLineageRow = {
  record_key: string
  batch_ref: string
  source: string
  status: string // 待校验 / 已通过 / 已隔离 / 已恢复 / 隔离中 / 已更正
  effective_reason: string | null
  quality_score: number | null
  validated: boolean
  recovered_at: string | null
  fields_masked: Record<string, string>
}

export type QualitySummary = {
  lineage_total: number
  current_issues: number
  historical_quarantine: number
  recovered: number
  corrected: number
  reason_details: ReasonDetail[]
  vault_quarantined: number
  vault_recovered: number
  reconciled: boolean
}

export type ReasonDetail = {
  reason: string
  current: number
  historical: number
  record_keys: string[]
}

export type RecordHistory = {
  record_key: string
  current_status: string
  current_reason: string | null
  current_record_id: number | null
  recovered_at: string | null
  recovered_to: string | null
  history: {
    record_id: number
    status: string
    reason: string
    detected_at: string
    recovered_at: string | null
  }[]
  recalc_events: {
    record_key: string
    reason: string
    detail: string
    event_at: string
    summary_deferred: boolean
  }[]
}

export type RecoverResult = {
  ok: boolean
  requested: number
  recovered: number
  skipped: { record_id: number; status: string }[]
  collect_synced: number
}

/** 当前问题：仍处于当前隔离状态、批量处理可更新的记录。 */
export function isCurrentIssue(row: QualityLineageRow): boolean {
  return row.status === '已隔离' || row.status === '隔离中'
}

/** 历史隔离：已恢复或已被更正取代的记录。 */
export function isHistorical(row: QualityLineageRow): boolean {
  return row.status === '已恢复' || row.status === '已更正'
}

/** 谱系列表统计：与质量汇总口径一致，按恢复状态区分当前与历史。 */
export function summarizeLineage(rows: QualityLineageRow[]) {
  return {
    total: rows.length,
    current: rows.filter(isCurrentIssue).length,
    historical: rows.filter(isHistorical).length,
  }
}
