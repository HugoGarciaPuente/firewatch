/**
 * Sección de estadísticas de FireWatch.
 *
 * Consume GET /api/v1/fire/locality-stats, que agrega en SQL por provincia y
 * municipio. No hay librería de gráficos: la evolución diaria se dibuja como
 * SVG y los rankings como barras CSS, para no añadir dependencias al dashboard.
 */

const RISK_META = {
  extreme: { label: 'Extremo', color: '#ef4444' },
  high: { label: 'Alto', color: '#f97316' },
  moderate: { label: 'Moderado', color: '#f59e0b' },
  low: { label: 'Bajo', color: '#10b981' },
};

const PERIOD_DAYS = {
  'last-day': 1,
  'last-week': 7,
  'last-month': 30,
  'last-year': 365,
};

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[ch]);
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  if (response.status === 401) {
    location.replace('/login');
    throw new Error('No autenticado');
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function shiftDate(iso, days) {
  const [year, month, day] = iso.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day + days));
  return date.toISOString().slice(0, 10);
}

class StatsPage {
  constructor() {
    this.provinceSelect = document.getElementById('stats-province');
    this.periodSelect = document.getElementById('stats-period');
    this.singleDate = document.getElementById('stats-single-date');
    this.startDate = document.getElementById('stats-start');
    this.endDate = document.getElementById('stats-end');
    this.singleField = document.getElementById('stats-single-field');
    this.rangeField = document.getElementById('stats-range-field');
    this.scope = document.getElementById('stats-scope');
    this.latestDate = null;
    this.firstDate = null;
  }

  async init() {
    document.getElementById('btn-refresh-stats').addEventListener('click', () => this.loadStats());
    document.getElementById('btn-logout').addEventListener('click', async () => {
      await fetch('/auth/logout', { method: 'POST' });
      location.replace('/login');
    });

    this.provinceSelect.addEventListener('change', () => this.loadStats());
    this.periodSelect.addEventListener('change', () => this.onPeriodChange());
    this.singleDate.addEventListener('change', () => this.loadStats());
    this.startDate.addEventListener('change', () => this.loadStats());
    this.endDate.addEventListener('change', () => this.loadStats());

    await Promise.all([this.loadProvinces(), this.loadLatest()]);
    this.onPeriodChange();
  }

  async loadProvinces() {
    try {
      const provinces = await fetchJson('/api/v1/fire/provinces');
      provinces.sort((a, b) => a.province.localeCompare(b.province, 'es'));
      for (const item of provinces) {
        const option = document.createElement('option');
        option.value = item.province;
        const foreign = item.country && item.country !== 'España';
        option.textContent = foreign ? `${item.province} (${item.country})` : item.province;
        this.provinceSelect.appendChild(option);
      }
    } catch (error) {
      console.warn('No se pudieron cargar las provincias:', error);
    }
  }

  async loadLatest() {
    try {
      const stats = await fetchJson('/api/v1/fire/stats');
      this.latestDate = stats.last_date;
      this.firstDate = stats.first_date;
    } catch (error) {
      console.warn('No se pudo obtener el último día con datos:', error);
    }

    if (!this.latestDate) {
      return;
    }

    this.singleDate.value = this.latestDate;
    this.endDate.value = this.latestDate;
    this.startDate.value = shiftDate(this.latestDate, -6);

    for (const input of [this.singleDate, this.startDate, this.endDate]) {
      if (this.firstDate) input.min = this.firstDate;
      input.max = this.latestDate;
    }
  }

  onPeriodChange(load = true) {
    const period = this.periodSelect.value;
    const isSingle = period === 'single';
    const isCustom = period === 'custom';

    this.singleField.hidden = !isSingle;
    this.rangeField.hidden = !isCustom;

    if (isSingle && !this.singleDate.value && this.latestDate) {
      this.singleDate.value = this.latestDate;
    }
    if (isCustom && !this.startDate.value && this.latestDate) {
      this.startDate.value = shiftDate(this.latestDate, -6);
      this.endDate.value = this.latestDate;
    }

    if (load) {
      this.loadStats();
    }
  }

  currentRange() {
    const period = this.periodSelect.value;

    if (period === 'all') {
      return {};
    }

    if (period === 'single') {
      return this.singleDate.value ? { date: this.singleDate.value } : null;
    }

    if (period === 'custom') {
      let start = this.startDate.value;
      let end = this.endDate.value;

      if (!start && !end) {
        return null;
      }
      if (start && end && start > end) {
        [start, end] = [end, start];
        this.startDate.value = start;
        this.endDate.value = end;
      }

      const range = {};
      if (start) range.start_date = start;
      if (end) range.end_date = end;
      return range;
    }

    if (!this.latestDate) {
      return null;
    }

    const days = PERIOD_DAYS[period];
    if (!days) {
      return {};
    }

    return {
      start_date: shiftDate(this.latestDate, -(days - 1)),
      end_date: this.latestDate,
    };
  }

  async loadStats() {
    const range = this.currentRange();

    if (!range) {
      this.scope.textContent = 'Selecciona una fecha';
      return;
    }

    const query = new URLSearchParams({ limit: '15', ...range });
    if (this.provinceSelect.value) {
      query.set('province', this.provinceSelect.value);
    }

    this.scope.textContent = 'Cargando…';

    try {
      const data = await fetchJson(`/api/v1/fire/locality-stats?${query.toString()}`);
      this.render(data);
      this.scope.textContent = this.describeScope(range, data);
    } catch (error) {
      this.scope.textContent = 'No se pudieron cargar las estadísticas';
      console.error(error);
    }
  }

  describeScope(range, data) {
    let period;

    if (this.periodSelect.value === 'single') {
      period = `Día ${this.formatDate(range.date)}`;
    } else if (range.start_date || range.end_date) {
      const start = range.start_date ? this.formatDate(range.start_date) : '…';
      const end = range.end_date ? this.formatDate(range.end_date) : '…';
      period = start === end ? `Día ${start}` : `${start} → ${end}`;
    } else {
      period = 'Todo el histórico';
    }

    const province = this.provinceSelect.value
      ? this.provinceSelect.selectedOptions[0].textContent
      : 'Todas las provincias';
    const empty = data.totals.events === 0 ? ' · sin datos' : '';

    return `${period} · ${province}${empty}`;
  }

  render(data) {
    const { totals, by_risk_level: levels, provinces, municipalities, daily } = data;

    document.getElementById('stat-total').textContent = this.number(totals.events);
    document.getElementById('stat-municipalities').textContent = this.number(totals.municipalities);
    document.getElementById('stat-provinces').textContent = this.number(totals.provinces);
    document.getElementById('stat-days').textContent = this.number(totals.days);
    document.getElementById('stat-high').textContent =
      this.number((levels.high || 0) + (levels.extreme || 0));
    document.getElementById('stat-range').textContent =
      totals.first_date && totals.last_date
        ? `${this.formatDate(totals.first_date)} → ${this.formatDate(totals.last_date)}`
        : 'Sin datos';

    this.renderRisk(levels);
    this.renderProvinces(provinces);
    this.renderMunicipalities(municipalities);
    this.renderDaily(daily);
  }

  renderRisk(levels) {
    const stack = document.getElementById('risk-stack');
    const legend = document.getElementById('risk-legend');
    const total = Object.values(levels).reduce((sum, count) => sum + count, 0);

    stack.innerHTML = '';
    legend.innerHTML = '';

    for (const [key, meta] of Object.entries(RISK_META)) {
      const count = levels[key] || 0;
      const pct = total ? (count / total) * 100 : 0;

      if (count) {
        const segment = document.createElement('div');
        segment.className = 'risk-segment';
        segment.style.width = `${pct}%`;
        segment.style.background = meta.color;
        segment.title = `${meta.label}: ${this.number(count)} (${pct.toFixed(1)}%)`;
        stack.appendChild(segment);
      }

      const item = document.createElement('span');
      item.className = 'risk-legend-item';
      item.innerHTML =
        `<span class="dot" style="background:${meta.color}"></span>` +
        `${meta.label} <strong>${this.number(count)}</strong> (${pct.toFixed(1)}%)`;
      legend.appendChild(item);
    }
  }

  renderProvinces(provinces) {
    const list = document.getElementById('provinces-list');
    if (!provinces.length) {
      list.innerHTML = '<div class="loading-placeholder">Sin datos de provincia</div>';
      return;
    }

    const max = Math.max(...provinces.map((province) => province.events), 1);
    list.innerHTML = provinces
      .map((province) => {
        let sub = '';
        if (province.country && province.country !== 'España') {
          sub = province.country;
        } else if (province.community && province.community !== province.province) {
          sub = province.community;
        }
        return `
          <div class="bar-row">
            <div class="bar-label" title="${escapeHtml(province.province)}">
              <span class="bar-name">${escapeHtml(province.province)}</span>
              <span class="bar-sub">${escapeHtml(sub)}</span>
            </div>
            <div class="bar-track"
                 title="${province.events} focos · ${province.municipalities} municipios · ${province.high} alto+">
              <div class="bar-fill" style="width:${((province.events / max) * 100).toFixed(1)}%"></div>
            </div>
            <div class="bar-value">${this.number(province.events)}</div>
          </div>`;
      })
      .join('');
  }

  renderMunicipalities(municipalities) {
    const body = document.getElementById('municipalities-body');
    if (!municipalities.length) {
      body.innerHTML =
        '<tr><td colspan="6" class="loading-placeholder">Sin datos de municipio</td></tr>';
      return;
    }

    body.innerHTML = municipalities
      .map((municipality, index) => `
        <tr>
          <td class="num">${index + 1}</td>
          <td><strong>${escapeHtml(municipality.municipality)}</strong></td>
          <td class="muted">${escapeHtml(this.placeLabel(municipality))}</td>
          <td class="num">${this.number(municipality.events)}</td>
          <td class="num">${this.number(municipality.high)}</td>
          <td class="num">${Number(municipality.max_risk_score || 0).toFixed(2)}</td>
        </tr>`)
      .join('');
  }

  placeLabel(item) {
    const place = item.province || item.country || '';
    if (item.country && item.country !== 'España') {
      return place && place !== item.country ? `${place} (${item.country})` : item.country;
    }
    return place;
  }

  renderDaily(daily) {
    const svg = document.getElementById('daily-chart');
    svg.innerHTML = '';
    if (!daily.length) {
      return;
    }

    const width = 800;
    const height = 240;
    const pad = { left: 48, right: 18, top: 18, bottom: 38 };
    const innerWidth = width - pad.left - pad.right;
    const innerHeight = height - pad.top - pad.bottom;
    const maxEvents = Math.max(...daily.map((day) => day.events), 1);
    const x = (index) =>
      daily.length === 1
        ? pad.left + innerWidth / 2
        : pad.left + (index / (daily.length - 1)) * innerWidth;
    const y = (value) => pad.top + innerHeight - (value / maxEvents) * innerHeight;

    const ns = 'http://www.w3.org/2000/svg';
    const add = (tag, attrs, text) => {
      const node = document.createElementNS(ns, tag);
      for (const [key, value] of Object.entries(attrs)) {
        node.setAttribute(key, value);
      }
      if (text !== undefined) {
        node.textContent = text;
      }
      svg.appendChild(node);
      return node;
    };

    // Rejilla horizontal y etiquetas del eje Y.
    for (const fraction of [0, 0.5, 1]) {
      const value = Math.round(maxEvents * fraction);
      const yPos = y(value);
      add('line', {
        class: 'grid',
        x1: pad.left,
        y1: yPos,
        x2: width - pad.right,
        y2: yPos,
      });
      add('text', { x: pad.left - 8, y: yPos + 3, 'text-anchor': 'end' }, value);
    }

    const points = daily.map((day, index) => [x(index), y(day.events)]);
    const line = points.map(([px, py]) => `${px.toFixed(1)},${py.toFixed(1)}`).join(' ');
    const baseline = pad.top + innerHeight;

    if (daily.length === 1) {
      // Un solo día: una barra se lee mejor que un punto suelto.
      const barWidth = Math.min(120, innerWidth / 3);
      const bar = add('rect', {
        class: 'bar',
        x: x(0) - barWidth / 2,
        y: y(daily[0].events),
        width: barWidth,
        height: Math.max(baseline - y(daily[0].events), 1),
        rx: 4,
      });
      const title = document.createElementNS(ns, 'title');
      title.textContent = `${this.formatDate(daily[0].date)}: ${daily[0].events} focos`;
      bar.appendChild(title);
    } else {
      add('polygon', {
        class: 'area',
        points: `${pad.left},${baseline} ${line} ${width - pad.right},${baseline}`,
      });
      add('polyline', { class: 'line', points: line });

      points.forEach(([px, py], index) => {
        const point = add('circle', { class: 'point', cx: px, cy: py, r: 3 });
        const title = document.createElementNS(ns, 'title');
        title.textContent = `${this.formatDate(daily[index].date)}: ${daily[index].events} focos`;
        point.appendChild(title);
      });
    }

    // Etiquetas del eje X: primera, central y última fecha.
    const labels =
      daily.length === 1
        ? [0]
        : [...new Set([0, Math.floor((daily.length - 1) / 2), daily.length - 1])];
    labels.forEach((index) => {
      let anchor = 'middle';
      if (daily.length > 1 && index === 0) {
        anchor = 'start';
      } else if (daily.length > 1 && index === daily.length - 1) {
        anchor = 'end';
      }
      add(
        'text',
        { x: x(index), y: height - 12, 'text-anchor': anchor },
        this.formatDate(daily[index].date),
      );
    });
  }

  formatDate(iso) {
    if (!iso) {
      return '—';
    }
    const [year, month, day] = iso.split('-');
    return `${day}/${month}/${year}`;
  }

  number(value) {
    return Number(value || 0).toLocaleString('es-ES');
  }
}

document.addEventListener('DOMContentLoaded', () => new StatsPage().init());
