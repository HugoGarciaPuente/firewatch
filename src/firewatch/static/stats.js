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

class StatsPage {
  constructor() {
    this.dateSelect = document.getElementById('stats-date');
    this.scope = document.getElementById('stats-scope');
  }

  async init() {
    document.getElementById('btn-refresh-stats').addEventListener('click', () => this.loadStats());
    document.getElementById('btn-logout').addEventListener('click', async () => {
      await fetch('/auth/logout', { method: 'POST' });
      location.replace('/login');
    });
    this.dateSelect.addEventListener('change', () => this.loadStats());

    await this.loadDates();
    await this.loadStats();
  }

  async loadDates() {
    try {
      const dates = await fetchJson('/api/v1/fire/dates');
      for (const day of dates) {
        const option = document.createElement('option');
        option.value = day.date;
        option.textContent = `${this.formatDate(day.date)} · ${this.number(day.total)} focos`;
        this.dateSelect.appendChild(option);
      }
    } catch (error) {
      // El selector vacío sigue funcionando como "todo el histórico".
      console.warn('No se pudieron cargar las fechas:', error);
    }
  }

  async loadStats() {
    const date = this.dateSelect.value;
    this.scope.textContent = 'Cargando…';
    try {
      const query = new URLSearchParams({ limit: '15' });
      if (date) {
        query.set('date', date);
      }
      const data = await fetchJson(`/api/v1/fire/locality-stats?${query.toString()}`);
      this.render(data);
      this.scope.textContent = date
        ? `Día ${this.formatDate(date)}`
        : `Histórico completo (${this.number(data.totals.days)} días)`;
    } catch (error) {
      this.scope.textContent = 'No se pudieron cargar las estadísticas';
      console.error(error);
    }
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
        const sub =
          province.community && province.community !== province.province
            ? province.community
            : province.country;
        return `
          <div class="bar-row">
            <div class="bar-label" title="${escapeHtml(province.province)}">
              <span class="bar-name">${escapeHtml(province.province)}</span>
              <span class="bar-sub">${escapeHtml(sub || '')}</span>
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
          <td class="muted">${escapeHtml(municipality.province || municipality.country)}</td>
          <td class="num">${this.number(municipality.events)}</td>
          <td class="num">${this.number(municipality.high)}</td>
          <td class="num">${Number(municipality.max_risk_score || 0).toFixed(2)}</td>
        </tr>`)
      .join('');
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
