(function () {
  'use strict';

  // --- State ---
  let availableDates = [];
  let currentDate = '';
  let currentView = 'day'; // day | week | month
  const cache = {}; // date -> JSON data
  let currentData = null; // current rendered data (for drill-down)

  // --- DOM refs ---
  const container = document.getElementById('main-container');
  const loading = document.getElementById('loading');
  const badge = document.getElementById('total-badge');
  const dateDisplay = document.getElementById('date-display');
  const datePicker = document.getElementById('date-picker');
  const prevBtn = document.getElementById('prev-btn');
  const nextBtn = document.getElementById('next-btn');
  const generatedInfo = document.getElementById('generated-info');
  const viewBtns = document.querySelectorAll('.view-btn');

  // --- Helpers ---
  function formatDatePL(dateStr) {
    const [y, m, d] = dateStr.split('-');
    return `${d}.${m}.${y}`;
  }

  function addDays(dateStr, n) {
    const d = new Date(dateStr + 'T12:00:00');
    d.setDate(d.getDate() + n);
    return d.toISOString().slice(0, 10);
  }

  function getMonday(dateStr) {
    const d = new Date(dateStr + 'T12:00:00');
    const day = d.getDay();
    const diff = day === 0 ? -6 : 1 - day;
    d.setDate(d.getDate() + diff);
    return d.toISOString().slice(0, 10);
  }

  function getMonthStart(dateStr) {
    return dateStr.slice(0, 8) + '01';
  }

  function getMonthEnd(dateStr) {
    const d = new Date(dateStr.slice(0, 7) + '-01T12:00:00');
    d.setMonth(d.getMonth() + 1);
    d.setDate(0);
    return d.toISOString().slice(0, 10);
  }

  function getDatesForRange() {
    if (currentView === 'day') {
      return [currentDate];
    }
    let start, end;
    if (currentView === 'week') {
      start = getMonday(currentDate);
      end = addDays(start, 6);
    } else {
      start = getMonthStart(currentDate);
      end = getMonthEnd(currentDate);
    }
    return availableDates.filter(d => d >= start && d <= end);
  }

  function getRangeLabel() {
    if (currentView === 'day') {
      return formatDatePL(currentDate);
    }
    if (currentView === 'week') {
      const mon = getMonday(currentDate);
      const sun = addDays(mon, 6);
      return `${formatDatePL(mon)} - ${formatDatePL(sun)}`;
    }
    const months = ['Stycze\u0144', 'Luty', 'Marzec', 'Kwiecie\u0144', 'Maj', 'Czerwiec',
      'Lipiec', 'Sierpie\u0144', 'Wrzesie\u0144', 'Pa\u017Adziernik', 'Listopad', 'Grudzie\u0144'];
    const [y, m] = currentDate.split('-');
    return `${months[parseInt(m) - 1]} ${y}`;
  }

  // --- Data fetching ---
  async function fetchJSON(url) {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    return resp.json();
  }

  async function loadIndex() {
    const data = await fetchJSON('data/index.json');
    if (data && data.dates) {
      availableDates = data.dates.sort();
    }
  }

  async function loadDayData(date) {
    if (cache[date]) return cache[date];
    const data = await fetchJSON(`data/${date}.json`);
    if (data) cache[date] = data;
    return data;
  }

  // --- Aggregation ---
  function pctStr(a, b) {
    return b > 0 ? `${Math.round(a / b * 100)}%` : '-';
  }

  function aggregateData(datasets) {
    if (datasets.length === 0) return null;
    if (datasets.length === 1) return datasets[0];

    const summary = {
      total: 0, new_lead: 0, mql: 0, sql: 0, won: 0,
      lost_before_mql: 0, sales_lost: 0, lost_total: 0,
      lead_mql_num: 0, mql_sql_num: 0, lead_sql_num: 0,
    };

    const sdrMap = {};
    const reasonMap = {};
    let activeSdrs = new Set();

    for (const data of datasets) {
      const s = data.summary;
      summary.total += s.total;
      summary.new_lead += s.new_lead;
      summary.mql += s.mql;
      summary.sql += s.sql;
      summary.won += s.won;
      summary.lost_before_mql += s.lost_before_mql;
      summary.sales_lost += s.sales_lost;
      summary.lost_total += s.lost_total;
      summary.lead_mql_num += (s.lead_mql_num || 0);
      summary.mql_sql_num += (s.mql_sql_num || 0);
      summary.lead_sql_num += (s.lead_sql_num || 0);

      for (const sdr of data.sdr_data) {
        activeSdrs.add(sdr.name);
        if (!sdrMap[sdr.name]) {
          sdrMap[sdr.name] = {
            name: sdr.name,
            stats: {
              total: 0, new_lead: 0, mql: 0, sql: 0, won: 0,
              lost_before_mql: 0, sales_lost: 0, lost_total: 0,
              lead_mql_num: 0, mql_sql_num: 0, lead_sql_num: 0,
            },
            deals: [],
            lost_deals: [],
          };
        }
        const m = sdrMap[sdr.name];
        const st = sdr.stats;
        m.stats.total += st.total;
        m.stats.new_lead += st.new_lead;
        m.stats.mql += st.mql;
        m.stats.sql += st.sql;
        m.stats.won += st.won;
        m.stats.lost_before_mql += st.lost_before_mql;
        m.stats.sales_lost += st.sales_lost;
        m.stats.lost_total += st.lost_total;
        m.stats.lead_mql_num += (st.lead_mql_num || 0);
        m.stats.mql_sql_num += (st.mql_sql_num || 0);
        m.stats.lead_sql_num += (st.lead_sql_num || 0);
        m.deals = m.deals.concat(sdr.deals);
        m.lost_deals = m.lost_deals.concat(sdr.lost_deals);
      }

      for (const lr of data.lost_reasons) {
        reasonMap[lr.reason] = (reasonMap[lr.reason] || 0) + lr.count;
      }
    }

    // Compute conversion strings
    summary.lead_mql = summary.new_lead > 0
      ? `${summary.lead_mql_num}/${summary.new_lead} (${pctStr(summary.lead_mql_num, summary.new_lead)})`
      : '-';
    summary.mql_sql = summary.mql > 0
      ? `${summary.mql_sql_num}/${summary.mql} (${pctStr(summary.mql_sql_num, summary.mql)})`
      : '-';
    summary.lead_sql = summary.new_lead > 0
      ? `${summary.lead_sql_num}/${summary.new_lead} (${pctStr(summary.lead_sql_num, summary.new_lead)})`
      : '-';

    // SDR data with conversion strings
    const sdr_data = Object.values(sdrMap)
      .sort((a, b) => b.stats.total - a.stats.total)
      .map(sdr => {
        const st = sdr.stats;
        st.lead_mql = st.new_lead > 0
          ? `${st.lead_mql_num}/${st.new_lead} (${pctStr(st.lead_mql_num, st.new_lead)})`
          : '-';
        st.mql_sql = st.mql > 0
          ? `${st.mql_sql_num}/${st.mql} (${pctStr(st.mql_sql_num, st.mql)})`
          : '-';
        st.lead_sql = st.new_lead > 0
          ? `${st.lead_sql_num}/${st.new_lead} (${pctStr(st.lead_sql_num, st.new_lead)})`
          : '-';
        return sdr;
      });

    const lost_reasons = Object.entries(reasonMap)
      .sort((a, b) => b[1] - a[1])
      .map(([reason, count]) => ({ reason, count }));

    return {
      date: datasets.length === 1 ? datasets[0].date : null,
      generated_at: datasets[datasets.length - 1].generated_at,
      summary,
      active_sdrs: activeSdrs.size,
      sdr_data,
      lost_reasons,
    };
  }

  // --- Rendering ---
  function escapeHTML(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // --- Filter mappings ---
  const FILTER_STAGE_MAP = {
    'new_lead': ['New Lead'],
    'mql': ['MQL'],
    'sql': ['Kwalka (SQL)'],
    'won': ['Sales Won'],
    'lost_before_mql': ['Lost Before MQL'],
    'sales_lost': ['Sales Lost'],
    'lost_total': ['Lost Before MQL', 'Sales Lost'],
  };

  const FILTER_LABELS = {
    'new_lead': 'Nowe Leady',
    'mql': 'MQL',
    'sql': 'SQL (Kwalka)',
    'won': 'Sales Won',
    'lost_before_mql': 'Lost Before MQL',
    'sales_lost': 'Sales Lost',
    'lost_total': 'Lost Total',
  };

  function getStageClass(stage) {
    const s = (stage || '').toLowerCase();
    if (s.includes('new lead')) return 'stage-new-lead';
    if (s.includes('in progress') || s.includes('call scheduled')) return 'stage-in-progress';
    if (s === 'mql') return 'stage-mql';
    if (s.includes('kwalka') || s.includes('sql')) return 'stage-sql';
    if (s.includes('won')) return 'stage-won';
    if (s.includes('lost')) return 'stage-lost';
    return 'stage-default';
  }

  function filterDeals(data, filterKey, sdrName) {
    const stages = FILTER_STAGE_MAP[filterKey];
    if (!stages || !data) return [];
    const deals = [];
    for (const sdr of data.sdr_data) {
      if (sdrName && sdr.name !== sdrName) continue;
      for (const d of sdr.deals) {
        const sc = d.stage_changes || [];
        if (stages.some(st => sc.includes(st))) {
          deals.push({ ...d, sdr_name: sdr.name });
        }
      }
    }
    return deals;
  }

  function openModal(title, deals) {
    const existing = document.querySelector('.modal-overlay');
    if (existing) existing.remove();

    if (deals.length === 0) return;

    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';

    let rows = '';
    for (const d of deals) {
      const stageClass = getStageClass(d.current_stage);
      rows += `
        <div class="modal-deal">
          <div>
            <div class="modal-deal-name">${escapeHTML((d.name || '').slice(0, 70))}</div>
            <div class="modal-deal-sdr">${escapeHTML(d.sdr_name)}</div>
          </div>
          <div class="modal-deal-stage ${stageClass}">${escapeHTML(d.current_stage)}</div>
        </div>`;
    }

    overlay.innerHTML = `
      <div class="modal">
        <div class="modal-header">
          <div style="display:flex;align-items:center">
            <h3>${escapeHTML(title)}</h3>
            <span class="modal-count">${deals.length}</span>
          </div>
          <button class="modal-close">&times;</button>
        </div>
        <div class="modal-body">${rows}</div>
      </div>`;

    document.body.appendChild(overlay);

    overlay.querySelector('.modal-close').addEventListener('click', () => overlay.remove());
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) overlay.remove();
    });
  }

  function renderDashboard(data) {
    currentData = data;

    if (!data) {
      container.innerHTML = `
        <div class="no-data">
          <h3>Brak danych</h3>
          <p>Brak danych dla wybranego zakresu. Wybierz inn\u0105 dat\u0119.</p>
        </div>`;
      badge.textContent = '-';
      generatedInfo.textContent = '';
      return;
    }

    const s = data.summary;
    badge.textContent = `${s.total} deali`;
    generatedInfo.textContent = data.generated_at ? `Wygenerowano: ${data.generated_at}` : '';

    let html = '';

    // KPI grid
    html += `
    <div class="kpi-grid">
      <div class="kpi-card blue" data-filter="new_lead"><div class="value">${s.new_lead}</div><div class="label">Nowe Leady</div></div>
      <div class="kpi-card purple" data-filter="mql"><div class="value">${s.mql}</div><div class="label">MQL</div></div>
      <div class="kpi-card green" data-filter="sql"><div class="value">${s.sql}</div><div class="label">SQL (Kwalka)</div></div>
      <div class="kpi-card green" data-filter="won"><div class="value">${s.won}</div><div class="label">Sales Won</div></div>
      <div class="kpi-card orange" data-filter="lost_before_mql"><div class="value">${s.lost_before_mql}</div><div class="label">Lost Before MQL</div></div>
      <div class="kpi-card red" data-filter="sales_lost"><div class="value">${s.sales_lost}</div><div class="label">Sales Lost</div></div>
      <div class="kpi-card red" data-filter="lost_total"><div class="value">${s.lost_total}</div><div class="label">Lost Total</div></div>
      <div class="kpi-card" style="cursor:default"><div class="value" style="color:#f1f5f9">${data.active_sdrs}</div><div class="label">Aktywni SDR-owie</div></div>
    </div>`;

    // Conversions
    html += `
    <div class="conv-grid">
      <div class="conv-card">
        <div class="conv-label">Lead <span class="conv-arrow">\u2192</span> MQL</div>
        <div class="conv-value">${escapeHTML(s.lead_mql)}</div>
      </div>
      <div class="conv-card">
        <div class="conv-label">MQL <span class="conv-arrow">\u2192</span> SQL</div>
        <div class="conv-value">${escapeHTML(s.mql_sql)}</div>
      </div>
      <div class="conv-card">
        <div class="conv-label">Lead <span class="conv-arrow">\u2192</span> SQL</div>
        <div class="conv-value">${escapeHTML(s.lead_sql)}</div>
      </div>
    </div>`;

    // SDR table
    html += `
    <div class="section">
      <h2>Konwersje per SDR</h2>
      <table>
        <thead>
          <tr>
            <th>SDR</th><th>Deale</th><th>New Lead</th><th>MQL</th>
            <th>SQL</th><th>Won</th><th>Lost</th>
            <th>Lead\u2192MQL</th><th>MQL\u2192SQL</th><th>Lead\u2192SQL</th>
          </tr>
        </thead>
        <tbody>`;

    for (const sdr of data.sdr_data) {
      const st = sdr.stats;
      const sn = escapeHTML(sdr.name);
      html += `
          <tr>
            <td>${sn}</td>
            <td>${st.total}</td>
            <td class="clickable" data-filter="new_lead" data-sdr="${sn}">${st.new_lead}</td>
            <td class="text-blue clickable" data-filter="mql" data-sdr="${sn}">${st.mql}</td>
            <td class="text-green clickable" data-filter="sql" data-sdr="${sn}">${st.sql}</td>
            <td class="text-green clickable" data-filter="won" data-sdr="${sn}">${st.won}</td>
            <td class="text-red clickable" data-filter="lost_total" data-sdr="${sn}">${st.lost_total}</td>
            <td class="text-blue">${escapeHTML(st.lead_mql)}</td>
            <td class="text-blue">${escapeHTML(st.mql_sql)}</td>
            <td class="text-blue">${escapeHTML(st.lead_sql)}</td>
          </tr>`;
    }

    html += `
        </tbody>
      </table>
    </div>`;

    // Lost reasons
    html += `<div class="section"><h2>Przyczyny Lost\u00f3w</h2>`;
    if (data.lost_reasons.length > 0) {
      const maxCount = data.lost_reasons[0].count;
      const totalLost = data.lost_reasons.reduce((sum, r) => sum + r.count, 0);
      for (const lr of data.lost_reasons) {
        const pct = Math.round(lr.count / totalLost * 100);
        const barW = Math.round(lr.count / maxCount * 100);
        html += `
        <div class="reason-bar">
          <div class="bar-label">${escapeHTML(lr.reason)}</div>
          <div class="bar-track">
            <div class="bar-fill" style="width:${barW}%">${pct}%</div>
          </div>
          <div class="bar-count">${lr.count}</div>
        </div>`;
      }
    } else {
      html += `<p style="color:#94a3b8">Brak lost\u00f3w w wybranym okresie</p>`;
    }
    html += `</div>`;

    // SDR detail cards
    html += `
    <div class="section">
      <h2>Szczeg\u00f3\u0142y per SDR</h2>
      <div class="sdr-cards">`;

    for (const sdr of data.sdr_data) {
      const st = sdr.stats;
      html += `
        <div class="sdr-card">
          <div class="sdr-card-header">
            <h3>${escapeHTML(sdr.name)}</h3>
            <span class="deal-count">${st.total} deali</span>
          </div>
          <div class="sdr-card-body">
            <div class="sdr-stat-row"><span class="sdr-stat-label">Nowe leady</span><span>${st.new_lead}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">MQL</span><span class="text-blue">${st.mql}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">SQL (Kwalka)</span><span class="text-green">${st.sql}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">Sales Won</span><span class="text-green">${st.won}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">Lost</span><span class="text-red">${st.lost_total}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">Lead \u2192 MQL</span><span class="text-blue">${escapeHTML(st.lead_mql)}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">MQL \u2192 SQL</span><span class="text-blue">${escapeHTML(st.mql_sql)}</span></div>
            <div class="sdr-stat-row"><span class="sdr-stat-label">Lead \u2192 SQL</span><span class="text-blue">${escapeHTML(st.lead_sql)}</span></div>`;

      // Lost deals details
      if (sdr.lost_deals && sdr.lost_deals.length > 0) {
        html += `<details><summary>Poka\u017C przyczyny lost\u00f3w</summary>`;
        for (const d of sdr.lost_deals) {
          html += `
            <div class="lost-item">
              <div class="deal-name">${escapeHTML((d.name || '').slice(0, 60))}</div>
              <div class="lost-meta">${escapeHTML(d.lost_type)} | ${escapeHTML(d.lost_reason)}</div>`;
          if (d.lost_description) {
            html += `<div class="lost-meta">${escapeHTML(d.lost_description.slice(0, 120))}</div>`;
          }
          html += `</div>`;
        }
        html += `</details>`;
      }

      // Deals list
      if (sdr.deals && sdr.deals.length > 0) {
        html += `<details><summary>Poka\u017C list\u0119 deali</summary><div style="margin-top:8px">`;
        for (const d of sdr.deals) {
          const stages = Array.isArray(d.stage_changes) ? d.stage_changes.join(', ') : '';
          html += `
            <div style="padding:4px 0;border-bottom:1px solid #263548;font-size:13px">
              <span style="color:#f1f5f9">${escapeHTML((d.name || '').slice(0, 50))}</span>
              <span style="color:#64748b;margin-left:8px">(${escapeHTML(d.current_stage)})</span>
              <div style="color:#94a3b8;font-size:11px">${escapeHTML(stages)}</div>
            </div>`;
        }
        html += `</div></details>`;
      }

      html += `
          </div>
        </div>`;
    }

    html += `</div></div>`;

    container.innerHTML = html;

    // Attach drill-down click handlers
    container.querySelectorAll('[data-filter]').forEach(el => {
      el.addEventListener('click', () => {
        const filterKey = el.dataset.filter;
        const sdrName = el.dataset.sdr || null;
        const deals = filterDeals(currentData, filterKey, sdrName);
        const label = FILTER_LABELS[filterKey] || filterKey;
        const title = sdrName ? `${label} - ${sdrName}` : label;
        openModal(title, deals);
      });
    });
  }

  function showLoading() {
    container.innerHTML = `
      <div class="loading">
        <div class="spinner"></div>
        <div>\u0141adowanie danych...</div>
      </div>`;
  }

  // --- Navigation ---
  function navigate(direction) {
    if (currentView === 'day') {
      const idx = availableDates.indexOf(currentDate);
      const newIdx = idx + direction;
      if (newIdx >= 0 && newIdx < availableDates.length) {
        currentDate = availableDates[newIdx];
      }
    } else if (currentView === 'week') {
      const mon = getMonday(currentDate);
      currentDate = addDays(mon, direction * 7);
    } else {
      const d = new Date(currentDate + 'T12:00:00');
      d.setMonth(d.getMonth() + direction);
      d.setDate(1);
      currentDate = d.toISOString().slice(0, 10);
    }
    updateUI();
  }

  function updateNavButtons() {
    if (availableDates.length === 0) {
      prevBtn.disabled = true;
      nextBtn.disabled = true;
      return;
    }
    const first = availableDates[0];
    const last = availableDates[availableDates.length - 1];

    if (currentView === 'day') {
      prevBtn.disabled = currentDate <= first;
      nextBtn.disabled = currentDate >= last;
    } else if (currentView === 'week') {
      // Disable only if the entire prev/next week is outside data range
      const prevWeekSun = addDays(getMonday(currentDate), -1);
      const nextWeekMon = addDays(getMonday(currentDate), 7);
      prevBtn.disabled = prevWeekSun < first;
      nextBtn.disabled = nextWeekMon > last;
    } else {
      // Disable only if prev/next month is outside data range
      const d = new Date(currentDate + 'T12:00:00');
      const prevMonth = new Date(d);
      prevMonth.setMonth(prevMonth.getMonth() - 1);
      const nextMonth = new Date(d);
      nextMonth.setMonth(nextMonth.getMonth() + 1);
      prevBtn.disabled = getMonthEnd(prevMonth.toISOString().slice(0, 10)) < first;
      nextBtn.disabled = getMonthStart(nextMonth.toISOString().slice(0, 10)) > last;
    }
  }

  // --- Main update ---
  async function updateUI() {
    dateDisplay.textContent = getRangeLabel();
    datePicker.value = currentDate;
    updateNavButtons();
    showLoading();

    const dates = getDatesForRange();

    if (dates.length === 0) {
      renderDashboard(null);
      return;
    }

    const datasets = [];
    for (const date of dates) {
      const data = await loadDayData(date);
      if (data) datasets.push(data);
    }

    if (datasets.length === 0) {
      renderDashboard(null);
      return;
    }

    const aggregated = aggregateData(datasets);
    renderDashboard(aggregated);
  }

  // --- Event listeners ---
  viewBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      viewBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentView = btn.dataset.view;
      updateUI();
    });
  });

  prevBtn.addEventListener('click', () => navigate(-1));
  nextBtn.addEventListener('click', () => navigate(1));

  datePicker.addEventListener('change', () => {
    const val = datePicker.value;
    if (val) {
      currentDate = val;
      // Snap to nearest available date for day view
      if (currentView === 'day' && availableDates.length > 0 && !availableDates.includes(val)) {
        const nearest = availableDates.reduce((prev, curr) =>
          Math.abs(new Date(curr) - new Date(val)) < Math.abs(new Date(prev) - new Date(val)) ? curr : prev
        );
        currentDate = nearest;
      }
      updateUI();
    }
  });

  // Keyboard navigation
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const modal = document.querySelector('.modal-overlay');
      if (modal) { modal.remove(); return; }
    }
    if (e.target.tagName === 'INPUT') return;
    if (e.key === 'ArrowLeft') navigate(-1);
    if (e.key === 'ArrowRight') navigate(1);
  });

  // --- Init ---
  async function init() {
    await loadIndex();

    if (availableDates.length > 0) {
      currentDate = availableDates[availableDates.length - 1]; // latest date
    } else {
      currentDate = new Date().toISOString().slice(0, 10);
    }

    await updateUI();
  }

  init();
})();
