import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import QualityWorkbenchView from '@/views/QualityWorkbenchView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 质量工作台是固定路径，必须先于 /:moduleKey 动态路由注册
    { path: '/quality', name: 'quality', component: QualityWorkbenchView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
