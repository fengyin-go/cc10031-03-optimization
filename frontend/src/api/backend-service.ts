import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

/** 质量工作台：去重后的采集质量谱系数据集（脱敏） */
export async function loadQualityLineage(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${encodeURIComponent(batchRef)}` : ''
  const response = await request(`/api/quality/lineage${query}`)
  if (!response.ok) throw new Error('质量谱系读取失败')
  return await response.json()
}

/** 质量工作台：按记录键查看当前隔离原因、恢复时间和历史重算记录 */
export async function loadQualityRecord(recordKey: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/quality/record?key=${encodeURIComponent(recordKey)}`)
  if (!response.ok) throw new Error('记录键详情读取失败')
  return await response.json()
}

/** 质量工作台：质量汇总（当前问题 / 历史隔离 / 隔离原因明细） */
export async function loadQualitySummary(): Promise<Record<string, unknown>> {
  const response = await request('/api/quality/summary')
  if (!response.ok) throw new Error('质量汇总读取失败')
  return await response.json()
}

/** 质量工作台：批量恢复（只更新当前状态记录，重复提交只算一次） */
export async function recoverQualityRecords(recordKeys: string[]): Promise<Record<string, unknown>> {
  const response = await request('/api/quality/recover', {
    method: 'POST',
    body: JSON.stringify({ record_keys: recordKeys }),
  })
  if (!response.ok) throw new Error('批量恢复失败')
  return await response.json()
}

/** 质量工作台：原始记录更正（rebuild 重建 / recalc_event 追加重算事件） */
export async function correctQualityRecord(
  recordKey: string,
  correctedFields: Record<string, unknown>,
  mode: 'rebuild' | 'recalc_event',
): Promise<Record<string, unknown>> {
  const response = await request('/api/quality/correct', {
    method: 'POST',
    body: JSON.stringify({ record_key: recordKey, corrected_fields: correctedFields, mode }),
  })
  if (!response.ok) throw new Error('记录更正失败')
  return await response.json()
}
