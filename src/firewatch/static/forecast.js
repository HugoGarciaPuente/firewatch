/**
 * Mapa provincial del pronóstico a 24 h.
 *
 * Consume GET /api/v1/fire/forecast y el GeoJSON administrativo de las 52
 * provincias. El color representa la probabilidad calibrada; la lista ordena
 * por score crudo, que es el que mejor rankea. Los polígonos son límites
 * administrativos, nunca perímetros de incendio.
 */

const FORECAST_COLOR_STOPS = [
  { p: 0.0, rgb: [30, 64, 175] },
  { p: 0.02, rgb: [8, 145, 178] },
  { p: 0.05, rgb: [245, 158, 11] },
  { p: 0.1, rgb: [249, 115, 22] },
  { p: 0.15, rgb: [239, 68, 68] },
  { p: 0.2, rgb: [153, 27, 27] },
];

const NO_DATA_COLOR = '#374151';

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[ch]);
}

function formatPercent(value) {
  const probability = Number(value);
  if (!Number.isFinite(probability) || probability <= 0) return '0.0%';
  const percent = probability * 100;
  if (percent < 0.1) return '<0.1%';
  return `${percent.toFixed(1)}%`;
}

function formatTargetDate(iso) {
  const [year, month, day] = iso.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  const weekdays = ['dom', 'lun', 'mar', 'mié', 'jue', 'vie', 'sáb'];
  const months = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];
  return `${weekdays[date.getUTCDay()]} ${day} ${months[month - 1]} ${year}`;
}

function colorForProbability(value) {
  const probability = Number(value);
  const stops = FORECAST_COLOR_STOPS;
  if (!Number.isFinite(probability) || probability <= stops[0].p) {
    return `rgb(${stops[0].rgb.join(',')})`;
  }
  if (probability >= stops[stops.length - 1].p) {
    return `rgb(${stops[stops.length - 1].rgb.join(',')})`;
  }

  for (let index = 1; index < stops.length; index += 1) {
    const previous = stops[index - 1];
    const next = stops[index];
    if (probability <= next.p) {
      const ratio = (probability - previous.p) / (next.p - previous.p);
      const rgb = previous.rgb.map((channel, channelIndex) => (
        Math.round(channel + (next.rgb[channelIndex] - channel) * ratio)
      ));
      return `rgb(${rgb.join(',')})`;
    }
  }
  return `rgb(${stops[stops.length - 1].rgb.join(',')})`;
}

async function fetchJson(url, accept = 'application/json') {
  const response = await fetch(url, { headers: { Accept: accept } });
  if (response.status === 401) {
    location.replace('/login');
    throw new Error('No autenticado');
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

class ForecastPage {
  constructor() {
    this.map = null;
    this.forecast = null;
    this.forecastByCode = new Map();
    this.provinceLayer = null;
    this.layersByCode = new Map();
    this.selectedCode = null;
  }

  async init() {
    this.bindEvents();
    this.initMap();
    await Promise.all([this.loadProvinces(), this.loadForecast()]);
    this.applyForecast();
  }

  bindEvents() {
    document.getElementById('btn-refresh-forecast').addEventListener('click', () => this.refresh());
    document.getElementById('btn-logout').addEventListener('click', async () => {
      await fetch('/auth/logout', { method: 'POST' });
      location.replace('/login');
    });

    document.querySelectorAll('.preset-regions .btn-chip').forEach((button) => {
      button.addEventListener('click', () => {
        document.querySelectorAll('.preset-regions .btn-chip').forEach((item) => {
          item.classList.remove('active');
        });
        button.classList.add('active');
        const [south, west, north, east] = button.dataset.bbox.split(',').map(Number);
        this.map.fitBounds([[south, west], [north, east]], { padding: [20, 20] });
      });
    });
  }

  initMap() {
    this.map = L.map('forecast-map', {
      center: [39.8, -3.8],
      zoom: 6,
      zoomControl: true,
    });

    L.tileLayer('https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png', {
      attribution:
        '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> &copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 19,
    }).addTo(this.map);

    this.map.fitBounds([[35.9, -9.6], [43.9, 3.6]], { padding: [20, 20] });
  }

  async loadProvinces() {
    const status = document.getElementById('forecast-map-status');
    try {
      const geojson = await fetchJson('/static/data/spain-provinces.geojson', 'application/geo+json');
      this.provinceLayer = L.geoJSON(geojson, {
        style: (feature) => this.styleFor(feature),
        onEachFeature: (feature, layer) => {
          const code = feature.properties.province_code;
          this.layersByCode.set(code, layer);
          layer.bindTooltip(this.tooltipFor(feature), {
            sticky: true,
            direction: 'top',
            className: 'forecast-tooltip',
          });
          layer.on({
            mouseover: () => {
              layer.setStyle({ weight: 2.5, color: '#ffffff', fillOpacity: 0.9 });
              layer.bringToFront();
            },
            mouseout: () => {
              layer.setStyle(this.styleFor(feature, this.selectedCode === code));
            },
            click: () => this.selectProvince(code),
          });
        },
      }).addTo(this.map);
      status.hidden = true;
    } catch (error) {
      status.textContent = `No se pudieron cargar los límites provinciales: ${error.message}`;
      console.error(error);
    }
  }

  async loadForecast() {
    const meta = document.getElementById('forecast-meta');
    const list = document.getElementById('forecast-list');
    const summary = document.getElementById('forecast-summary');
    meta.textContent = 'Cargando pronóstico...';
    list.innerHTML = '<div class="loading-placeholder">Consultando modelo...</div>';
    summary.innerHTML = '';

    try {
      const response = await fetch('/api/v1/fire/forecast', {
        headers: { Accept: 'application/json' },
      });
      if (response.status === 401) {
        location.replace('/login');
        throw new Error('No autenticado');
      }
      if (response.status === 404) {
        this.forecast = null;
        this.forecastByCode.clear();
        meta.textContent = 'Sin pronóstico disponible. Ejecuta runner forecast.';
        list.innerHTML =
          '<div class="loading-placeholder">Todavía no hay pronóstico guardado.</div>';
        return;
      }
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      this.forecast = await response.json();
      this.forecastByCode = new Map(
        this.forecast.provinces.map((province) => [province.province_code, province])
      );
      meta.textContent =
        `${formatTargetDate(this.forecast.target_date)} · ` +
        `${this.forecast.count} provincias · modelo ` +
        `${this.forecast.model_name}-${this.forecast.model_version}`;
    } catch (error) {
      this.forecast = null;
      this.forecastByCode.clear();
      meta.textContent = 'Pronóstico no disponible';
      list.innerHTML =
        `<div class="loading-placeholder" style="color:#ef4444;">` +
        `Error al cargar el pronóstico: ${escapeHtml(error.message)}</div>`;
    }
  }

  applyForecast() {
    if (this.provinceLayer) {
      this.provinceLayer.eachLayer((layer) => {
        const code = layer.feature.properties.province_code;
        layer.setStyle(this.styleFor(layer.feature, this.selectedCode === code));
        layer.setTooltipContent(this.tooltipFor(layer.feature));
      });
    }
    this.renderSummary();
    this.renderList();
  }

  async refresh() {
    const button = document.getElementById('btn-refresh-forecast');
    button.disabled = true;
    try {
      if (!this.provinceLayer) await this.loadProvinces();
      await this.loadForecast();
      this.applyForecast();
    } finally {
      button.disabled = false;
    }
  }

  styleFor(feature, selected = false) {
    const record = this.forecastByCode.get(feature.properties.province_code);
    const probability = record ? Number(record.probability_calibrated || 0) : null;
    return {
      fillColor: probability == null ? NO_DATA_COLOR : colorForProbability(probability),
      fillOpacity: probability == null ? 0.22 : (selected ? 0.92 : 0.72),
      color: selected ? '#ffffff' : 'rgba(255,255,255,0.42)',
      weight: selected ? 2.5 : 1,
      opacity: 1,
    };
  }

  tooltipFor(feature) {
    const record = this.forecastByCode.get(feature.properties.province_code);
    const name = escapeHtml(record?.province_name || feature.properties.province_name);
    if (!record) {
      return `<strong>${name}</strong><br><span class="forecast-tooltip-muted">Sin pronóstico</span>`;
    }
    const calibrated = Number(record.probability_calibrated || 0);
    const raw = Number(record.probability || 0);
    return `
      <strong>${name}</strong><br>
      <span class="forecast-tooltip-prob" style="color:${colorForProbability(calibrated)}">${formatPercent(calibrated)}</span>
      <span class="forecast-tooltip-muted">calibrada</span><br>
      <span class="forecast-tooltip-muted">raw ${raw.toFixed(4)}</span>
    `;
  }

  renderSummary() {
    const summary = document.getElementById('forecast-summary');
    if (!this.forecast || !this.forecast.provinces.length) {
      summary.innerHTML = '';
      return;
    }

    const values = this.forecast.provinces.map((province) => (
      Number(province.probability_calibrated || 0)
    ));
    const maximum = Math.max(...values);
    const mean = values.reduce((total, value) => total + value, 0) / values.length;
    const top = this.forecast.provinces.reduce((best, province) => (
      Number(province.probability_calibrated || 0) > Number(best.probability_calibrated || 0)
        ? province
        : best
    ));

    summary.innerHTML = `
      <div class="forecast-kpi">
        <span class="forecast-kpi-label">Máxima</span>
        <span class="forecast-kpi-value">${formatPercent(maximum)}</span>
        <span class="forecast-kpi-hint">${escapeHtml(top.province_name)}</span>
      </div>
      <div class="forecast-kpi">
        <span class="forecast-kpi-label">Media</span>
        <span class="forecast-kpi-value">${formatPercent(mean)}</span>
        <span class="forecast-kpi-hint">entre provincias</span>
      </div>
      <div class="forecast-kpi">
        <span class="forecast-kpi-label">Provincias</span>
        <span class="forecast-kpi-value">${this.forecast.count}</span>
        <span class="forecast-kpi-hint">con pronóstico</span>
      </div>
    `;
  }

  renderList() {
    const list = document.getElementById('forecast-list');
    if (!this.forecast || !this.forecast.provinces.length) {
      list.innerHTML = '<div class="loading-placeholder">Sin pronóstico disponible.</div>';
      return;
    }

    list.innerHTML = '';
    this.forecast.provinces.forEach((province, index) => {
      const calibrated = Number(province.probability_calibrated || 0);
      const raw = Number(province.probability || 0);
      const row = document.createElement('button');
      row.type = 'button';
      row.className = 'forecast-row';
      row.dataset.code = province.province_code;
      row.title = `Score crudo ${raw.toFixed(4)}`;
      row.setAttribute(
        'aria-label',
        `${province.province_name}: ${formatPercent(calibrated)} calibrada`
      );
      row.innerHTML = `
        <span class="forecast-rank">${index + 1}</span>
        <span class="forecast-place">
          <span class="forecast-province">
            <span class="forecast-swatch" style="background:${colorForProbability(calibrated)}"></span>
            ${escapeHtml(province.province_name)}
          </span>
          <span class="forecast-raw">raw ${raw.toFixed(4)}</span>
        </span>
        <span class="forecast-prob">${formatPercent(calibrated)}</span>
      `;
      row.addEventListener('click', () => this.focusProvince(province.province_code));
      list.appendChild(row);
    });

    if (this.selectedCode) {
      this.highlightRow(this.selectedCode);
    }
  }

  focusProvince(code) {
    const layer = this.layersByCode.get(code);
    if (layer) {
      this.map.fitBounds(layer.getBounds(), { padding: [30, 30], maxZoom: 9 });
    }
    this.selectProvince(code);
  }

  selectProvince(code) {
    this.selectedCode = code;
    this.highlightRow(code);

    if (!this.provinceLayer) return;
    this.provinceLayer.eachLayer((layer) => {
      const featureCode = layer.feature.properties.province_code;
      layer.setStyle(this.styleFor(layer.feature, featureCode === code));
      if (featureCode === code) {
        layer.bringToFront();
        layer.openTooltip();
      }
    });
  }

  highlightRow(code) {
    document.querySelectorAll('.forecast-row').forEach((row) => {
      const selected = row.dataset.code === code;
      row.classList.toggle('selected', selected);
      if (selected) row.scrollIntoView({ block: 'nearest' });
    });
  }
}

new ForecastPage().init().catch((error) => {
  const status = document.getElementById('forecast-map-status');
  if (status) {
    status.hidden = false;
    status.textContent = `Error al cargar el pronóstico: ${error.message}`;
  }
  console.error(error);
});
