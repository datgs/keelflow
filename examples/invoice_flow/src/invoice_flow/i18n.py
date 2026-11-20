"""UI translation catalog for the invoice_flow demo.

Five locales: en, vi, es, zh-Hans, hi. English is the source catalog —
other locales must cover the same key set. Locale resolves from the
`lang` query param first, then the `kf_lang` cookie, then English.

Only presentation strings live here. Workflow/job/state identifiers,
actor ids, permission names, CSV field keys, and demo fixture values are
never translated.
"""
from __future__ import annotations

LOCALES = ("en", "vi", "es", "zh-Hans", "hi")
DEFAULT_LOCALE = "en"
COOKIE_NAME = "kf_lang"

_EN = {
    "nav.invoices": "Invoices",
    "nav.inbox": "Inbox",
    "header.view_as": "View as",
    "header.can": "can:",
    "header.demo_only": "demo only, no auth",
    "list.new_invoice": "New invoice",
    "list.vendor_placeholder": "Vendor",
    "list.create": "Create",
    "list.col.number": "Number",
    "list.col.vendor": "Vendor",
    "list.col.amount": "Amount",
    "list.col.state": "State",
    "list.open": "open →",
    "footer.roles_hint": ("Roles: u1=clerk (submit) · fin1=finance (approve) · "
                          "gl1=GL check · mgr=all. Switch user in the header."),
    "banner.waiting_human": "Waiting on human",
    "banner.assignee": "assignee:",
    "banner.open_inbox": "open Inbox",
    "inv.created": "created",
    "inv.gl": "GL:",
    "inv.posted": "Posted",
    "inv.posted_by": "by",
    "inv.no_actions": "no actions for {me} in state {state}",
    "inv.fail_next": "fail next posting",
    "inv.fail_next_hint": "demo: simulate posting failure",
    "inv.resume_run": "resume run",
    "action.submit": "submit",
    "action.resubmit": "resubmit",
    "action.approve": "approve",
    "action.repost": "repost",
    "action.retry_post": "retry_post",
    "wf.title": "Posting workflow",
    "wf.outcome": "outcome:",
    "wf.callback": "callback:",
    "wf.no_run": "no run yet — approve the invoice to trigger post.wf@v1",
    "audit.title": "Audit trail",
    "audit.col.when": "When",
    "audit.col.actor": "Actor",
    "audit.col.action": "Action",
    "audit.col.changes": "Changes",
    "audit.empty": "empty",
    "sm.title": "State machine — you are here: {state}",
    "dag.human_gate_legend": "dashed pill = human gate → assignee",
    "dag.title": "Workflow DAG — latest run live status",
    "inbox.title": "My tasks ({me})",
    "inbox.col.task": "Task",
    "inbox.col.invoice": "Invoice",
    "inbox.col.assignee": "Assignee",
    "inbox.col.claimed": "Claimed",
    "inbox.col.actions": "Actions",
    "inbox.unclaimed": "unclaimed",
    "inbox.claimed_by": "claimed by {who}",
    "inbox.claim": "claim",
    "inbox.claim_hint": "then choose an action",
    "inbox.approve": "approve",
    "inbox.reject": "reject",
    "inbox.repost": "re-post",
    "inbox.empty": "no open tasks for {me} — check role assignment"
                   " (gl tasks → gl1, finance → fin1)",
    "inbox.note": ("First claim wins — claiming as one user blocks completion"
                   " by others. gl_check waits in the posting DAG; completing"
                   " it finishes the run."),
    "task.human.review@v1": "Finance review",
    "task.human.gl_check@v1": "GL check (posting)",
    "task.human.posting_fix@v1": "Posting fix-up",
    "run.col.job": "job →",
    "run.blocked_hint": ("A blocked step is a human task parked in an inbox —"
                         " completing it unblocks the run. Refresh to see"
                         " progress (manual refresh in this demo)."),
    "run.record": "Run record",
    "run.steps": "Steps",
    "run.id": "id",
    "job.attempts": "attempts",
    "job.key": "key",
    "job.assignee": "assignee",
    "job.retry": "retry",
    "job.payload": "Payload",
    "job.result": "Result",
    "job.context": "Context (actor + via chain)",
}

_VI = {
    "nav.invoices": "Hóa đơn",
    "nav.inbox": "Hộp thư",
    "header.view_as": "Xem với vai trò",
    "header.can": "quyền:",
    "header.demo_only": "chỉ demo, không xác thực",
    "list.new_invoice": "Hóa đơn mới",
    "list.vendor_placeholder": "Nhà cung cấp",
    "list.create": "Tạo",
    "list.col.number": "Số",
    "list.col.vendor": "Nhà cung cấp",
    "list.col.amount": "Số tiền",
    "list.col.state": "Trạng thái",
    "list.open": "mở →",
    "footer.roles_hint": ("Vai trò: u1=clerk (gửi) · fin1=finance (duyệt) · "
                          "gl1=kiểm tra GL · mgr=tất cả. Đổi người dùng ở "
                          "thanh trên."),
    "banner.waiting_human": "Đang chờ con người xử lý",
    "banner.assignee": "người được giao:",
    "banner.open_inbox": "mở Hộp thư",
    "inv.created": "tạo lúc",
    "inv.gl": "GL:",
    "inv.posted": "Đã ghi sổ",
    "inv.posted_by": "bởi",
    "inv.no_actions": "không có hành động cho {me} ở trạng thái {state}",
    "inv.fail_next": "lỗi bút toán kế tiếp",
    "inv.fail_next_hint": "demo: giả lập lỗi ghi sổ",
    "inv.resume_run": "chạy lại",
    "action.submit": "gửi",
    "action.resubmit": "gửi lại",
    "action.approve": "phê duyệt",
    "action.repost": "gửi lại bút toán",
    "action.retry_post": "thử lại",
    "wf.title": "Quy trình ghi sổ",
    "wf.outcome": "kết quả:",
    "wf.callback": "callback:",
    "wf.no_run": "chưa có run — duyệt hóa đơn để kích hoạt post.wf@v1",
    "audit.title": "Nhật ký kiểm toán",
    "audit.col.when": "Thời gian",
    "audit.col.actor": "Người thao tác",
    "audit.col.action": "Hành động",
    "audit.col.changes": "Thay đổi",
    "audit.empty": "trống",
    "sm.title": "Máy trạng thái — vị trí hiện tại: {state}",
    "dag.human_gate_legend": "ô nét đứt = cổng chờ người → người được giao",
    "dag.title": "DAG quy trình — trạng thái run mới nhất",
    "inbox.title": "Việc của tôi ({me})",
    "inbox.col.task": "Tác vụ",
    "inbox.col.invoice": "Hóa đơn",
    "inbox.col.assignee": "Người được giao",
    "inbox.col.claimed": "Đã nhận",
    "inbox.col.actions": "Hành động",
    "inbox.unclaimed": "chưa nhận",
    "inbox.claimed_by": "{who} đã nhận",
    "inbox.claim": "nhận xử lý",
    "inbox.claim_hint": "rồi chọn hành động",
    "inbox.approve": "phê duyệt",
    "inbox.reject": "từ chối",
    "inbox.repost": "gửi lại",
    "inbox.empty": "không có tác vụ mở cho {me} — kiểm tra phân quyền"
                   " (task gl → gl1, finance → fin1)",
    "inbox.note": ("Ai nhận trước được xử lý — một người nhận sẽ chặn người"
                   " khác hoàn tất. gl_check chờ trong DAG ghi sổ; hoàn tất"
                   " nó sẽ kết thúc run."),
    "task.human.review@v1": "Finance duyệt",
    "task.human.gl_check@v1": "Kiểm tra GL (ghi sổ)",
    "task.human.posting_fix@v1": "Sửa lỗi ghi sổ",
    "run.col.job": "job →",
    "run.blocked_hint": ("Bước blocked là tác vụ con người đang chờ trong"
                         " hộp thư — hoàn tất nó sẽ mở khóa run. Tải lại"
                         " trang để xem tiến độ (demo tự refresh)."),
    "run.record": "Bản ghi run",
    "run.steps": "Các bước",
    "run.id": "id",
    "job.attempts": "lần thử",
    "job.key": "khóa",
    "job.assignee": "người được giao",
    "job.retry": "thử lại",
    "job.payload": "Payload",
    "job.result": "Kết quả",
    "job.context": "Ngữ cảnh (actor + chuỗi via)",
}

_ES = {
    "nav.invoices": "Facturas",
    "nav.inbox": "Bandeja",
    "header.view_as": "Ver como",
    "header.can": "permisos:",
    "header.demo_only": "solo demo, sin autenticación",
    "list.new_invoice": "Nueva factura",
    "list.vendor_placeholder": "Proveedor",
    "list.create": "Crear",
    "list.col.number": "Número",
    "list.col.vendor": "Proveedor",
    "list.col.amount": "Importe",
    "list.col.state": "Estado",
    "list.open": "abrir →",
    "footer.roles_hint": ("Roles: u1=clerk (enviar) · fin1=finance (aprobar) · "
                          "gl1=revisión GL · mgr=todo. Cambia el usuario en el"
                          " encabezado."),
    "banner.waiting_human": "Esperando acción humana",
    "banner.assignee": "asignado a:",
    "banner.open_inbox": "abrir Bandeja",
    "inv.created": "creada",
    "inv.gl": "GL:",
    "inv.posted": "Contabilizado",
    "inv.posted_by": "por",
    "inv.no_actions": "sin acciones para {me} en estado {state}",
    "inv.fail_next": "fallar siguiente asiento",
    "inv.fail_next_hint": "demo: simular fallo de contabilización",
    "inv.resume_run": "reanudar run",
    "action.submit": "enviar",
    "action.resubmit": "reenviar",
    "action.approve": "aprobar",
    "action.repost": "recontabilizar",
    "action.retry_post": "reintentar",
    "wf.title": "Flujo de contabilización",
    "wf.outcome": "resultado:",
    "wf.callback": "callback:",
    "wf.no_run": "aún sin run — aprueba la factura para iniciar post.wf@v1",
    "audit.title": "Registro de auditoría",
    "audit.col.when": "Cuándo",
    "audit.col.actor": "Actor",
    "audit.col.action": "Acción",
    "audit.col.changes": "Cambios",
    "audit.empty": "vacío",
    "sm.title": "Máquina de estados — estás aquí: {state}",
    "dag.human_gate_legend": "píldora discontinua = tarea humana → asignado a",
    "dag.title": "DAG del flujo — estado del último run",
    "inbox.title": "Mis tareas ({me})",
    "inbox.col.task": "Tarea",
    "inbox.col.invoice": "Factura",
    "inbox.col.assignee": "Asignado a",
    "inbox.col.claimed": "Reclamada",
    "inbox.col.actions": "Acciones",
    "inbox.unclaimed": "sin reclamar",
    "inbox.claimed_by": "reclamada por {who}",
    "inbox.claim": "reclamar",
    "inbox.claim_hint": "y luego elige una acción",
    "inbox.approve": "aprobar",
    "inbox.reject": "rechazar",
    "inbox.repost": "recontabilizar",
    "inbox.empty": "sin tareas abiertas para {me} — revisa la asignación de"
                   " roles (tareas gl → gl1, finance → fin1)",
    "inbox.note": ("Quien reclama primero gana — reclamar como un usuario"
                   " impide que otros completen. gl_check espera en el DAG de"
                   " contabilización; completarlo finaliza el run."),
    "task.human.review@v1": "Revisión de finance",
    "task.human.gl_check@v1": "Revisión GL (asiento)",
    "task.human.posting_fix@v1": "Corrección de asiento",
    "run.col.job": "job →",
    "run.blocked_hint": ("Un paso blocked es una tarea humana en espera en la"
                         " bandeja — completarla desbloquea el run. Actualiza"
                         " para ver el progreso (actualización manual en esta"
                         " demo)."),
    "run.record": "Registro del run",
    "run.steps": "Pasos",
    "run.id": "id",
    "job.attempts": "intentos",
    "job.key": "clave",
    "job.assignee": "asignado a",
    "job.retry": "reintentar",
    "job.payload": "Payload",
    "job.result": "Resultado",
    "job.context": "Contexto (actor + cadena via)",
}

_ZH = {
    "nav.invoices": "发票",
    "nav.inbox": "收件箱",
    "header.view_as": "切换身份",
    "header.can": "权限：",
    "header.demo_only": "仅供演示，无认证",
    "list.new_invoice": "新建发票",
    "list.vendor_placeholder": "供应商",
    "list.create": "创建",
    "list.col.number": "编号",
    "list.col.vendor": "供应商",
    "list.col.amount": "金额",
    "list.col.state": "状态",
    "list.open": "打开 →",
    "footer.roles_hint": ("角色：u1=clerk（提交）· fin1=finance（批准）· "
                          "gl1=GL 复核 · mgr=全部。在页眉切换用户。"),
    "banner.waiting_human": "等待人工处理",
    "banner.assignee": "指派给：",
    "banner.open_inbox": "打开收件箱",
    "inv.created": "创建时间",
    "inv.gl": "GL:",
    "inv.posted": "已过账",
    "inv.posted_by": "操作人",
    "inv.no_actions": "{me} 在 {state} 状态下没有可用操作",
    "inv.fail_next": "下次过账失败",
    "inv.fail_next_hint": "演示：模拟过账失败",
    "inv.resume_run": "恢复运行",
    "action.submit": "提交",
    "action.resubmit": "重新提交",
    "action.approve": "批准",
    "action.repost": "重新过账",
    "action.retry_post": "重试",
    "wf.title": "过账工作流",
    "wf.outcome": "结果：",
    "wf.callback": "回调：",
    "wf.no_run": "暂无运行 — 批准发票以触发 post.wf@v1",
    "audit.title": "审计日志",
    "audit.col.when": "时间",
    "audit.col.actor": "操作者",
    "audit.col.action": "操作",
    "audit.col.changes": "变更",
    "audit.empty": "空",
    "sm.title": "状态机 — 当前状态：{state}",
    "dag.human_gate_legend": "虚线标签 = 人工节点 → 指派给",
    "dag.title": "工作流 DAG — 最新运行实时状态",
    "inbox.title": "我的任务（{me}）",
    "inbox.col.task": "任务",
    "inbox.col.invoice": "发票",
    "inbox.col.assignee": "指派给",
    "inbox.col.claimed": "认领人",
    "inbox.col.actions": "操作",
    "inbox.unclaimed": "未认领",
    "inbox.claimed_by": "已由 {who} 认领",
    "inbox.claim": "认领",
    "inbox.claim_hint": "然后选择操作",
    "inbox.approve": "批准",
    "inbox.reject": "驳回",
    "inbox.repost": "重新过账",
    "inbox.empty": "{me} 没有待办任务 — 请检查角色分配"
                   "（gl 任务 → gl1，finance → fin1）",
    "inbox.note": ("先到先得 — 某用户认领后其他人无法完成。gl_check 在过账"
                   " DAG 中等待；完成后运行即结束。"),
    "task.human.review@v1": "财务审核",
    "task.human.gl_check@v1": "GL 复核（过账）",
    "task.human.posting_fix@v1": "过账修复",
    "run.col.job": "job →",
    "run.blocked_hint": ("blocked 步骤是收件箱中的人工任务 — 完成后解除运行"
                         "阻塞。刷新页面查看进度（本演示为手动刷新）。"),
    "run.record": "运行记录",
    "run.steps": "步骤",
    "run.id": "id",
    "job.attempts": "重试次数",
    "job.key": "键",
    "job.assignee": "指派给",
    "job.retry": "重试",
    "job.payload": "Payload",
    "job.result": "结果",
    "job.context": "上下文（actor + via 链）",
}

_HI = {
    "nav.invoices": "इनवॉइस",
    "nav.inbox": "इनबॉक्स",
    "header.view_as": "के रूप में देखें",
    "header.can": "अनुमतियाँ:",
    "header.demo_only": "केवल डेमो, प्रमाणीकरण नहीं",
    "list.new_invoice": "नई इनवॉइस",
    "list.vendor_placeholder": "विक्रेता",
    "list.create": "बनाएँ",
    "list.col.number": "संख्या",
    "list.col.vendor": "विक्रेता",
    "list.col.amount": "राशि",
    "list.col.state": "स्थिति",
    "list.open": "खोलें →",
    "footer.roles_hint": ("भूमिकाएँ: u1=clerk (सबमिट) · fin1=finance (स्वीकृति)"
                          " · gl1=GL जाँच · mgr=सभी। हेडर में उपयोगकर्ता बदलें।"),
    "banner.waiting_human": "मानव कार्रवाई की प्रतीक्षा",
    "banner.assignee": "असाइन किया गया:",
    "banner.open_inbox": "इनबॉक्स खोलें",
    "inv.created": "बनाया गया",
    "inv.gl": "GL:",
    "inv.posted": "पोस्ट हुआ",
    "inv.posted_by": "द्वारा",
    "inv.no_actions": "{state} स्थिति में {me} के लिए कोई क्रिया नहीं",
    "inv.fail_next": "अगली पोस्टिंग विफल करें",
    "inv.fail_next_hint": "डेमो: पोस्टिंग विफलता का अनुकरण",
    "inv.resume_run": "run फिर से चलाएँ",
    "action.submit": "सबमिट",
    "action.resubmit": "फिर सबमिट",
    "action.approve": "स्वीकृत करें",
    "action.repost": "फिर पोस्ट करें",
    "action.retry_post": "पुनः प्रयास",
    "wf.title": "पोस्टिंग वर्कफ़्लो",
    "wf.outcome": "परिणाम:",
    "wf.callback": "कॉलबैक:",
    "wf.no_run": "अभी कोई run नहीं — post.wf@v1 शुरू करने के लिए इनवॉइस स्वीकृत करें",
    "audit.title": "ऑडिट ट्रेल",
    "audit.col.when": "कब",
    "audit.col.actor": "कर्ता",
    "audit.col.action": "क्रिया",
    "audit.col.changes": "परिवर्तन",
    "audit.empty": "खाली",
    "sm.title": "स्टेट मशीन — वर्तमान स्थिति: {state}",
    "dag.human_gate_legend": "डैश किया हुआ बॉक्स = मानव गेट → असाइन किया गया",
    "dag.title": "वर्कफ़्लो DAG — नवीनतम run की लाइव स्थिति",
    "inbox.title": "मेरे कार्य ({me})",
    "inbox.col.task": "कार्य",
    "inbox.col.invoice": "इनवॉइस",
    "inbox.col.assignee": "असाइन किया गया",
    "inbox.col.claimed": "दावा किया",
    "inbox.col.actions": "क्रियाएँ",
    "inbox.unclaimed": "दावा नहीं किया",
    "inbox.claimed_by": "{who} द्वारा दावा किया",
    "inbox.claim": "दावा करें",
    "inbox.claim_hint": "फिर कोई क्रिया चुनें",
    "inbox.approve": "स्वीकृत करें",
    "inbox.reject": "अस्वीकार करें",
    "inbox.repost": "फिर पोस्ट करें",
    "inbox.empty": "{me} के लिए कोई खुला कार्य नहीं — भूमिका असाइनमेंट जाँचें"
                   " (gl कार्य → gl1, finance → fin1)",
    "inbox.note": ("जो पहले दावा करता है वही जीतता है — एक उपयोगकर्ता का दावा"
                   " दूसरों को पूरा करने से रोकता है। gl_check पोस्टिंग DAG"
                   " में इंतज़ार करता है; उसे पूरा करने पर run समाप्त होता है।"),
    "task.human.review@v1": "फ़ाइनेंस समीक्षा",
    "task.human.gl_check@v1": "GL जाँच (पोस्टिंग)",
    "task.human.posting_fix@v1": "पोस्टिंग सुधार",
    "run.col.job": "job →",
    "run.blocked_hint": ("blocked चरण इनबॉक्स में प्रतीक्षारत मानव कार्य है —"
                         " उसे पूरा करने पर run अनब्लॉक होता है। प्रगति देखने"
                         " के लिए रीफ्रेश करें (इस डेमो में मैन्युअल रीफ्रेश)।"),
    "run.record": "रन रिकॉर्ड",
    "run.steps": "चरण",
    "run.id": "id",
    "job.attempts": "प्रयास",
    "job.key": "कुंजी",
    "job.assignee": "असाइन किया गया",
    "job.retry": "पुनः प्रयास",
    "job.payload": "Payload",
    "job.result": "परिणाम",
    "job.context": "संदर्भ (actor + via श्रृंखला)",
}

_STRINGS: dict[str, dict[str, str]] = {
    "en": _EN, "vi": _VI, "es": _ES, "zh-Hans": _ZH, "hi": _HI}


def resolve_locale(request) -> str:
    """`?lang=` beats the `kf_lang` cookie; both must be in LOCALES."""
    lang = request.query_params.get("lang")
    if lang in LOCALES:
        return lang
    if request.cookies.get(COOKIE_NAME) in LOCALES:
        return request.cookies[COOKIE_NAME]
    return DEFAULT_LOCALE


def translator(locale: str):
    """Return t(key, **kwargs) → translated string, English fallback."""
    strings = _STRINGS.get(locale, _EN)

    def t(key: str, **kw) -> str:
        s = strings.get(key) or _EN.get(key) or key
        return s.format(**kw) if kw else s

    return t


def render(templates, request, name: str, context: dict):
    """TemplateResponse with lang/t/locales injected; sets kf_lang cookie
    when the user picked a locale via ?lang=."""
    lang = resolve_locale(request)
    ctx = {"lang": lang, "t": translator(lang),
           "locales": LOCALES, **context}
    resp = templates.TemplateResponse(request, name, ctx)
    picked = request.query_params.get("lang")
    if picked in LOCALES:
        resp.set_cookie(COOKIE_NAME, picked)
    return resp
