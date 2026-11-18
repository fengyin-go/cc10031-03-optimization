<template>
  <section class="page" data-module="quality_workbench">
    <header class="page-head">
      <div>
        <h2>质量工作台</h2>
        <p class="page-desc">
          核对去重后的采集质量谱系：按记录键查看当前隔离原因、恢复时间与历史重算记录，
          批量处理只更新仍处于当前隔离状态的记录。
        </p>
      </div>
      <div class="page-actions">
        <button
          class="btn primary"
          data-testid="batch-recover"
          type="button"
          :disabled="recovering || selectedKeys.length === 0"
          @click="recoverSelected"
        >
          {{ recovering ? '恢复中…' : `批量恢复（${selectedKeys.length}）` }}
        </button>
        <button class="btn" data-testid="refresh" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">谱系记录键</span>
        <strong class="stat-value" data-testid="stat-total">{{ summary.lineage_total ?? stats.total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">当前问题</span>
        <strong class="stat-value" data-testid="stat-current">{{ summary.current_issues ?? stats.current }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">历史隔离</span>
        <strong class="stat-value" data-testid="stat-historical">{{ summary.historical_quarantine ?? stats.historical }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已恢复</span>
        <strong class="stat-value" data-testid="stat-recovered">{{ summary.recovered ?? 0 }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reloadAll">
      <label class="filter-item">
        <span>批次编号</span>
        <input v-model="batchFilter" data-testid="filter-batch" placeholder="按批次过滤谱系" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilter">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>选择</th>
          <th>记录键</th>
          <th>采集批次</th>
          <th>有效状态</th>
          <th>当前隔离原因</th>
          <th>恢复时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.record_key" :data-testid="`row-${row.record_key}`" :data-status="row.status">
          <td>
            <input
              v-if="isCurrentIssue(row)"
              v-model="selected"
              type="checkbox"
              :value="row.record_key"
              :data-testid="`select-${row.record_key}`"
            />
            <span v-else>—</span>
          </td>
          <td>{{ row.record_key }}</td>
          <td>{{ row.batch_ref }}</td>
          <td>{{ row.status }}</td>
          <td>{{ row.effective_reason ?? '—' }}</td>
          <td>{{ row.recovered_at ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`detail-${row.record_key}`" @click="openDetail(row.record_key)">
              查看详情
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td colspan="7" class="empty-state">暂无采集质量谱系数据，可先执行采集校验链路</td>
        </tr>
      </tbody>
    </table>

    <section v-if="detail" class="detail-panel" data-testid="record-detail">
      <h3>记录键详情：{{ detail.record_key }}</h3>
      <dl class="detail-grid">
        <dt>当前状态</dt>
        <dd data-testid="detail-status">{{ detail.current_status }}</dd>
        <dt>当前隔离原因</dt>
        <dd data-testid="detail-reason">{{ detail.current_reason ?? '—' }}</dd>
        <dt>恢复时间</dt>
        <dd data-testid="detail-recovered-at">{{ detail.recovered_at ?? '—' }}</dd>
        <dt>恢复去向</dt>
        <dd>{{ detail.recovered_to ?? '—' }}</dd>
      </dl>
      <h4>隔离履历</h4>
      <table class="data-table">
        <thead>
          <tr><th>隔离单号</th><th>状态</th><th>隔离原因</th><th>隔离时间</th><th>恢复时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.history" :key="item.record_id">
            <td>{{ item.record_id }}</td>
            <td>{{ item.status }}</td>
            <td>{{ item.reason }}</td>
            <td>{{ item.detected_at }}</td>
            <td>{{ item.recovered_at ?? '—' }}</td>
          </tr>
          <tr v-if="!detail.history.length"><td colspan="5" class="empty-state">无隔离履历</td></tr>
        </tbody>
      </table>
      <h4>历史重算记录</h4>
      <table class="data-table">
        <thead>
          <tr><th>重算原因</th><th>说明</th><th>登记时间</th><th>汇总延迟</th></tr>
        </thead>
        <tbody>
          <tr v-for="(event, index) in detail.recalc_events" :key="index">
            <td>{{ event.reason }}</td>
            <td>{{ event.detail }}</td>
            <td>{{ event.event_at }}</td>
            <td>{{ event.summary_deferred ? '已延迟' : '否' }}</td>
          </tr>
          <tr v-if="!detail.recalc_events.length"><td colspan="4" class="empty-state">无重算记录</td></tr>
        </tbody>
      </table>
    </section>

    <section class="reason-panel">
      <h3>隔离原因明细</h3>
      <table class="data-table">
        <thead>
          <tr><th>隔离原因</th><th>当前</th><th>历史</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in reasonDetails" :key="item.reason">
            <td>{{ item.reason }}</td>
            <td>{{ item.current }}</td>
            <td>{{ item.historical }}</td>
          </tr>
          <tr v-if="!reasonDetails.length"><td colspan="3" class="empty-state">暂无隔离原因记录</td></tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ rows.length }} 条谱系记录</span>
      <span v-if="lastMessage" class="ok-text" data-testid="action-message">{{ lastMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  loadQualityLineage,
  loadQualityRecord,
  loadQualitySummary,
  recoverQualityRecords,
} from '@/api/backend-service'
import {
  isCurrentIssue,
  summarizeLineage,
  type QualityLineageRow,
  type QualitySummary,
  type ReasonDetail,
  type RecordHistory,
  type RecoverResult,
} from '@/domain/quality'

const rows = ref<QualityLineageRow[]>([])
const summary = ref<Partial<QualitySummary>>({})
const reasonDetails = ref<ReasonDetail[]>([])
const detail = ref<RecordHistory | null>(null)
const batchFilter = ref('')
const selected = ref<string[]>([])
const recovering = ref(false)
const errorMessage = ref('')
const lastMessage = ref('')

const selectedKeys = computed(() => selected.value)
const stats = computed(() => summarizeLineage(rows.value))

async function reloadAll() {
  // 处理详情与结果清单同步更新：汇总、谱系、详情一并刷新
  errorMessage.value = ''
  try {
    const [lineage, sum] = await Promise.all([
      loadQualityLineage(batchFilter.value || undefined),
      loadQualitySummary(),
    ])
    rows.value = (lineage.items as QualityLineageRow[]) ?? []
    summary.value = sum as QualitySummary
    reasonDetails.value = (sum.reason_details as ReasonDetail[]) ?? []
    const validKeys = new Set(rows.value.map((row) => row.record_key))
    selected.value = selected.value.filter((key) => validKeys.has(key))
    if (detail.value) {
      const fresh = await loadQualityRecord(detail.value.record_key)
      detail.value = fresh as unknown as RecordHistory
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '质量谱系读取失败'
  }
}

function resetFilter() {
  batchFilter.value = ''
  void reloadAll()
}

async function openDetail(recordKey: string) {
  errorMessage.value = ''
  try {
    const fresh = await loadQualityRecord(recordKey)
    detail.value = fresh as unknown as RecordHistory
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '记录键详情读取失败'
  }
}

async function recoverSelected() {
  if (recovering.value || selected.value.length === 0) return
  recovering.value = true // 并发两次提交只算一次：提交中禁止重复触发
  errorMessage.value = ''
  lastMessage.value = ''
  try {
    const result = (await recoverQualityRecords([...selected.value])) as unknown as RecoverResult
    lastMessage.value = `已恢复 ${result.recovered} 条，跳过 ${result.skipped.length} 条`
    selected.value = []
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量恢复失败'
  } finally {
    recovering.value = false
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.detail-panel,
.reason-panel {
  margin-top: 16px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
}
.detail-panel h3,
.reason-panel h3 {
  margin: 0 0 10px;
  font-size: 14px;
}
.detail-panel h4 {
  margin: 12px 0 6px;
  font-size: 13px;
}
.detail-grid {
  display: grid;
  grid-template-columns: 120px 1fr 120px 1fr;
  gap: 6px 10px;
  margin: 0 0 8px;
}
.detail-grid dt {
  color: var(--muted);
  font-size: 12px;
}
.detail-grid dd {
  margin: 0;
  font-size: 13px;
}
.ok-text {
  color: #15803d;
}
</style>
