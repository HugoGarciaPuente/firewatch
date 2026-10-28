/**
 * Aplicación del Dashboard GIS FireWatch
 *
 * Visualización (siempre activa, sin conmutadores):
 *   - Puntos (circle markers) por nivel de riesgo
 *   - Heatmap (leaflet.heat)            -> densidad de anomalías
 *   - Clusters (leaflet.markercluster)  -> agrupación por zoom
 *
 * Filtros:
 *   - Día de adquisición (granularidad diaria, selector tipo calendario)
 *   - Nivel de riesgo
 *   - Puntuación mínima
 */

const RISK_COLORS = {
  extreme: '#ef4444',
  high: '#f97316',
  moderate: '#f59e0b',
  low: '#10b981',
};

class FireWatchApp {
  constructor() {
    this.map = null;
    this.markersLayer = null; // layerGroup de circle markers
    this.clusterLayer = null; // markerClusterGroup
    this.heatmapLayer = null; // L.heatLayer
    this.events = [];
    this.selectedEvent = null;
    this.selectedDate = null; // día activo; null = todos
    this.history = []; // desglose por día desde /dates
    this.calYear = new Date().getUTCFullYear();
    this.calMonth = new Date().getUTCMonth();

    this.initMap();
    this.initEventListeners();
    this.loadData();
    this.startHealthPolling();
  }

  initMap() {
    this.map = L.map('map', {
      center: [39.8, -3.8],
      zoom: 6,
      zoomControl: true,
    });

    // Única capa base: modo oscuro Stadia Maps.
    L.tileLayer('https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png', {
      attribution:
        '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> &copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 19,
    }).addTo(this.map);

    this.markersLayer = L.layerGroup().addTo(this.map);
    this.clusterLayer = L.markerClusterGroup({
      showCoverageOnHover: false,
      maxClusterRadius: 45,
      disableClusteringAtZoom: 12,
    });
  }

  initEventListeners() {
    document.getElementById('btn-refresh').addEventListener('click', () => this.loadData());

    // --- Filtros ---
    document.getElementById('filter-risk').addEventListener('change', () => this.loadEvents());

    const scoreSlider = document.getElementById('filter-score');
    scoreSlider.addEventListener('input', (e) => {
      document.getElementById('score-val').textContent = parseFloat(e.target.value).toFixed(2);
    });
    scoreSlider.addEventListener('change', () => this.loadEvents());

    // --- Calendario ---
    document.getElementById('cal-prev-month').addEventListener('click', () => this.shiftMonth(-1));
    document.getElementById('cal-next-month').addEventListener('click', () => this.shiftMonth(1));
    const btnDayLast = document.getElementById('btn-day-last');
    if (btnDayLast) {
      btnDayLast.addEventListener('click', () => {
        if (!this.history.length) return;
        const newest = this.history[0].date;
        if (this.selectedDate === newest) return;
        this.setDate(newest);
        this.loadStats();
        this.loadEvents();
      });
    }

    // Logout
    const logoutBtn = document.getElementById('btn-logout');
    if (logoutBtn) {
      logoutBtn.addEventListener('click', async () => {
        await fetch('/auth/logout', { method: 'POST' });
        location.replace('/login');
      });
    }

    // Presets de región
    document.querySelectorAll('.preset-regions .btn-chip').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        document.querySelectorAll('.preset-regions .btn-chip').forEach((b) => b.classList.remove('active'));
        e.target.classList.add('active');
        const bbox = e.target.dataset.bbox.split(',').map(Number);
        this.map.fitBounds([
          [bbox[0], bbox[1]],
          [bbox[2], bbox[3]],
        ]);
      });
    });

    document.getElementById('btn-close-inspector').addEventListener('click', () => {
      document.getElementById('inspector-panel').style.display = 'none';
      this.selectedEvent = null;
      this.highlightSelectedInList();
    });

  }

  async loadData() {
    await this.checkSystemHealth();
    await this.loadAvailableDates();
    await Promise.all([
      this.loadStats(),
      this.loadEvents(),
    ]);
  }

  async checkSystemHealth() {
    const statusEl = document.getElementById('system-status');
    const dot = statusEl.querySelector('.status-dot');
    const text = statusEl.querySelector('.status-text');

    try {
      const res = await fetch('/health/db');
      if (res.ok) {
        dot.className = 'status-dot online';
        text.textContent = 'Servidor conectado';
      } else {
        dot.className = 'status-dot offline';
        text.textContent = 'BD No Conectada';
      }
    } catch {
      dot.className = 'status-dot offline';
      text.textContent = 'Servidor Inaccesible';
    }
  }

  startHealthPolling() {
    setInterval(() => this.checkSystemHealth(), 15000);
  }

  /** KPIs superiores: respetan el día seleccionado. */
  async loadStats() {
    try {
      const params = this.selectedDate ? `?date=${this.selectedDate}` : '';
      const res = await fetch(`/api/v1/fire/stats${params}`);
      if (!res.ok) return;
      const data = await res.json();

      document.getElementById('kpi-total').textContent = data.total ?? 0;
      const byLevel = data.by_risk_level || {};
      document.getElementById('kpi-extreme').textContent = byLevel.extreme || 0;
      document.getElementById('kpi-high').textContent = byLevel.high || 0;
      document.getElementById('kpi-moderate').textContent = byLevel.moderate || 0;
      document.getElementById('kpi-low').textContent = byLevel.low || 0;

      const rangeEl = document.getElementById('kpi-date-range');
      if (data.total === 0) {
        rangeEl.textContent = this.selectedDate ? 'Sin datos este día' : 'Sin datos';
      } else if (this.selectedDate) {
        rangeEl.textContent = `Score máx ${(data.max_risk_score || 0).toFixed(2)}`;
      } else if (data.first_date && data.last_date) {
        rangeEl.textContent =
          data.first_date === data.last_date
            ? data.first_date
            : `${data.first_date} → ${data.last_date}`;
      } else {
        rangeEl.textContent = 'Sin datos';
      }
    } catch (err) {
      console.error('Error loading stats:', err);
    }
  }

  /**
   * Histórico por día (fechas + conteo + desglose de riesgo) que alimenta el
   * selector tipo calendario.
   */
  async loadAvailableDates() {
    try {
      const res = await fetch('/api/v1/fire/dates');
      if (!res.ok) return;
      this.history = await res.json();

      const gridEl = document.getElementById('calendar-grid');
      if (!this.history.length) {
        if (gridEl) gridEl.innerHTML = '<div class="loading-placeholder">Sin histórico.</div>';
        this.selectedDate = null;
        this.renderDateSummary();
        return;
      }

      // Si no hay día seleccionado, o el día seleccionado ya no existe en el histórico,
      // seleccionar por defecto el último día con datos (el más reciente).
      if (!this.selectedDate || !this.history.some((d) => d.date === this.selectedDate)) {
        this.selectedDate = this.history[0].date;
      }

      // El calendario arranca en el mes del día seleccionado.
      const current = this.selectedDate;
      this.calYear = +current.slice(0, 4);
      this.calMonth = +current.slice(5, 7) - 1;
      this.renderCalendar();
      this.renderDateSummary();
    } catch (err) {
      console.warn('No se pudo cargar el histórico:', err);
    }
  }

  /** Fija el día activo (o null para "todos") y refresca mapa + KPIs. */
  setDate(date) {
    this.selectedDate = date;
    this.renderCalendar();
    this.renderDateSummary();
  }

  /** Cambia el mes visible del calendario. */
  shiftMonth(delta) {
    const d = new Date(Date.UTC(this.calYear, this.calMonth + delta, 1));
    this.calYear = d.getUTCFullYear();
    this.calMonth = d.getUTCMonth();

    // No dejar navegar más allá de los meses que tienen datos.
    const min = this.history.length ? this.history[this.history.length - 1].date : null;
    const max = this.history.length ? this.history[0].date : null;
    const atMin = this.calYear * 12 + this.calMonth <= (min ? +min.slice(0, 4) * 12 + (+min.slice(5, 7) - 1) : 0);
    const atMax = this.calYear * 12 + this.calMonth >= (max ? +max.slice(0, 4) * 12 + (+max.slice(5, 7) - 1) : 0);

    document.getElementById('cal-prev-month').disabled = atMin;
    document.getElementById('cal-next-month').disabled = atMax;

    this.renderCalendar();
  }

  /**
   * Calendario mensual. Solo se habilitan los días que tienen registros:
   * el resto se muestra atenuado para que se vea la cobertura real.
   */
  renderCalendar() {
    const grid = document.getElementById('calendar-grid');
    const label = document.getElementById('cal-month-label');
    if (!grid) return;

    const monthNames = [
      'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
      'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
    ];
    label.textContent = `${monthNames[this.calMonth]} ${this.calYear}`;

    const byDate = new Map(this.history.map((d) => [d.date, d]));
    const first = new Date(Date.UTC(this.calYear, this.calMonth, 1));
    const daysInMonth = new Date(Date.UTC(this.calYear, this.calMonth + 1, 0)).getUTCDate();
    const leading = (first.getUTCDay() + 6) % 7; // lunes = 0

    grid.innerHTML = '';

    // Huecos del inicio de mes
    for (let i = 0; i < leading; i++) {
      const empty = document.createElement('div');
      empty.className = 'cal-day empty';
      grid.appendChild(empty);
    }

    for (let day = 1; day <= daysInMonth; day++) {
      const iso = `${this.calYear}-${String(this.calMonth + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
      const info = byDate.get(iso);

      const cell = document.createElement('button');
      cell.type = 'button';
      cell.className = 'cal-day';

      if (!info) {
        cell.classList.add('no-data');
        cell.disabled = true;
        cell.textContent = String(day);
      } else {
        const worst = this.worstLevel(info.by_risk_level);
        cell.classList.add('has-data', `lvl-${worst}`);
        if (this.selectedDate === iso) {
          cell.classList.add('selected');
          cell.setAttribute('aria-pressed', 'true');
        } else {
          cell.setAttribute('aria-pressed', 'false');
        }
        cell.title =
          `${iso}\n${info.total} focos\n` +
          `Bajo ${info.by_risk_level.low || 0} · Moderado ${info.by_risk_level.moderate || 0} · ` +
          `Alto ${info.by_risk_level.high || 0} · Extremo ${info.by_risk_level.extreme || 0}`;

        cell.innerHTML = `<span class="cal-day-num">${day}</span><span class="cal-dot"></span>`;

        cell.addEventListener('click', () => {
          if (this.selectedDate === iso) return;
          this.setDate(iso);
          this.loadStats();
          this.loadEvents();
        });
      }

      grid.appendChild(cell);
    }
  }

  renderDateSummary() {
    const main = document.querySelector('#date-summary .date-summary-main');
    const sub = document.getElementById('date-summary-sub');
    const btnLast = document.getElementById('btn-day-last');
    if (!main) return;

    if (btnLast && this.history.length > 0) {
      if (this.selectedDate === this.history[0].date) {
        btnLast.classList.add('active');
      } else {
        btnLast.classList.remove('active');
      }
    }

    if (!this.selectedDate) {
      main.textContent = 'Sin datos';
      sub.textContent = 'No hay registros disponibles';
      return;
    }

    const day = this.history.find((d) => d.date === this.selectedDate);
    if (!day) {
      main.textContent = this.selectedDate;
      sub.textContent = 'Sin datos este día';
      return;
    }

    const worst = this.worstLevel(day.by_risk_level);
    main.innerHTML = `${this.formatDayLabel(day.date)} <span class="event-risk-badge badge-${worst}">${worst}</span>`;
    sub.textContent = `${day.total} focos · score máx ${(day.max_risk_score || 0).toFixed(2)}`;
  }

  /** "mié 1 oct" a partir de YYYY-MM-DD. */
  formatDayLabel(iso) {
    const [y, m, d] = iso.split('-').map(Number);
    const dt = new Date(Date.UTC(y, m - 1, d));
    const dow = ['dom', 'lun', 'mar', 'mié', 'jue', 'vie', 'sáb'][dt.getUTCDay()];
    const month = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'][m - 1];
    return `${dow} ${d} ${month}`;
  }

  worstLevel(byLevel = {}) {
    if (byLevel.extreme) return 'extreme';
    if (byLevel.high) return 'high';
    if (byLevel.moderate) return 'moderate';
    return 'low';
  }

  async loadEvents() {
    const riskLevel = document.getElementById('filter-risk').value;
    const minScore = parseFloat(document.getElementById('filter-score').value) || 0;

    const params = new URLSearchParams();
    if (riskLevel) params.append('risk_level', riskLevel);
    if (minScore > 0) params.append('min_risk_score', String(minScore));
    if (this.selectedDate) params.append('date', this.selectedDate);
    params.append('limit', '500');

    const listContainer = document.getElementById('events-list');
    listContainer.innerHTML = '<div class="loading-placeholder">Consultando focos...</div>';

    try {
      const res = await fetch(`/api/v1/fire/events?${params.toString()}`);
      if (!res.ok) throw new Error('Error en consulta');
      this.events = await res.json();

      document.getElementById('event-count').textContent = this.events.length;
      this.renderMarkers();
      this.renderEventsList();
      this.updateHeatmap();
      this.updateClusters();
    } catch (err) {
      listContainer.innerHTML = `<div class="loading-placeholder" style="color:#ef4444;">Error al cargar eventos: ${err.message}</div>`;
    }
  }

  // ------------------------------------------------------------------
  // Renderizado de capas
  // ------------------------------------------------------------------

  renderMarkers() {
    this.markersLayer.clearLayers();

    this.events.forEach((evt) => {
      const risk = (evt.risk_level || 'low').toLowerCase();
      const color = RISK_COLORS[risk] || RISK_COLORS.low;
      const radius = 7 + (evt.risk_score || 0) * 12;

      const circle = L.circleMarker([evt.latitude, evt.longitude], {
        radius: radius,
        fillColor: color,
        color: '#ffffff',
        weight: 1.5,
        opacity: 0.9,
        fillOpacity: 0.7,
      });

      circle.bindPopup(this.buildPopupHtml(evt));
      circle.on('click', () => this.selectEvent(evt));
      circle.addTo(this.markersLayer);
    });
  }

  /**
   * Formatea la localidad reverse-geocodificada.
   *
   * El ámbito es España y el procesador descarta los puntos cuyo país
   * geocodificado queda fuera, así que en la práctica el país no se muestra.
   * La rama extranjera sigue aquí como red de seguridad para filas históricas o
   * para un `FIREWATCH_COUNTRY_CODES` más amplio: sin ella, un foco de Portugal
   * aparecería como si fuera de una provincia española.
   *
   * Devuelve HTML escapado o null si el punto todavía no tiene localidad
   * (aún sin pasar por `geocode-backfill`, o sin resolver en Nominatim).
   */
  formatLocality(evt) {
    const loc = evt.locality;
    if (!loc || (!loc.locality && !loc.display_name)) return null;

    const name = loc.locality || loc.display_name.split(',')[0];
    const province = loc.province || '';
    const foreign = loc.country_code && loc.country_code !== 'es';

    const parts = [name];
    if (province && province !== name) parts.push(province);
    if (foreign && loc.country) parts.push(loc.country);

    return parts
      .filter(Boolean)
      .map((p) => this.escapeHtml(String(p)))
      .join(' · ');
  }

  /** Escapa texto que va a innerHTML. */
  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  buildPopupHtml(evt) {
    const risk = (evt.risk_level || 'low').toLowerCase();
    const f = evt.risk_factors || {};
    const place = this.formatLocality(evt);
    return `
      <div class="fire-popup">
        <h4>🔥 Foco Térmico <span class="event-risk-badge badge-${risk}">${risk}</span></h4>
        ${
          place
            ? `<div><strong>Localidad:</strong> ${place}</div>`
            : `<div><strong>Localidad:</strong> <span class="muted">sin resolver</span></div>`
        }
        <div><strong>Fecha:</strong> ${evt.acq_date}</div>
        <div><strong>Coord:</strong> ${evt.latitude.toFixed(4)}, ${evt.longitude.toFixed(4)}</div>
        <div><strong>Brillo:</strong> ${evt.brightness ? evt.brightness.toFixed(1) + ' K' : 'N/D'}</div>
        <div><strong>Riesgo:</strong> ${(evt.risk_score || 0).toFixed(2)}</div>
        ${
          f.temperature_c != null
            ? `<div><strong>Clima:</strong> ${f.temperature_c}°C | Hum: ${f.relative_humidity}% | Viento: ${f.wind_speed_kmh} km/h</div>`
            : ''
        }
        ${
          f.fuel_moisture_pct != null
            ? `<div><strong>Humedad combustible:</strong> ${f.fuel_moisture_pct}% (propagación: ${f.spread_index})</div>`
            : ''
        }
      </div>
    `;
  }

  /**
   * Heatmap: densidad de anomalías ponderada por riesgo. Cada foco aporta un
   * peso proporcional a su risk_score, de forma que las zonas más peligrosas
   * concentran más intensidad.
   */
  updateHeatmap() {
    if (this.heatmapLayer) {
      this.map.removeLayer(this.heatmapLayer);
      this.heatmapLayer = null;
    }
    if (!this.events.length) return;

    const points = this.events.map((e) => {
      const w = (e.risk_score || 0.1) + 0.1; // peso mínimo > 0 para que entre en el kernel
      return [e.latitude, e.longitude, w];
    });

    this.heatmapLayer = L.heatLayer(points, {
      radius: 28,
      blur: 22,
      maxZoom: 12,
      minOpacity: 0.35,
      gradient: {
        0.2: '#10b981',
        0.4: '#f59e0b',
        0.6: '#f97316',
        1.0: '#ef4444',
      },
    }).addTo(this.map);
  }

  /** Clusters: siempre activos, sustituyen a los puntos individuales. */
  updateClusters() {
    if (this.map.hasLayer(this.markersLayer)) this.map.removeLayer(this.markersLayer);

    this.clusterLayer.clearLayers();
    this.clusterLayer.addLayers(
      this.events.map((evt) => {
        const risk = (evt.risk_level || 'low').toLowerCase();
        const color = RISK_COLORS[risk] || RISK_COLORS.low;
        const marker = L.circleMarker([evt.latitude, evt.longitude], {
          radius: 8,
          fillColor: color,
          color: '#ffffff',
          weight: 1.5,
          fillOpacity: 0.85,
        });
        marker.bindPopup(this.buildPopupHtml(evt));
        marker.on('click', () => this.selectEvent(evt));
        return marker;
      })
    );

    if (!this.map.hasLayer(this.clusterLayer)) this.clusterLayer.addTo(this.map);
  }

  // ------------------------------------------------------------------
  // Lista / inspector
  // ------------------------------------------------------------------

  renderEventsList() {
    const container = document.getElementById('events-list');
    if (!this.events.length) {
      container.innerHTML = '<div class="loading-placeholder">No hay focos para el día seleccionado.</div>';
      return;
    }

    container.innerHTML = '';
    this.events.forEach((evt) => {
      const risk = (evt.risk_level || 'low').toLowerCase();
      const place = this.formatLocality(evt);
      const card = document.createElement('div');
      card.className = `event-card ${this.selectedEvent === evt ? 'selected' : ''}`;
      card.innerHTML = `
        <div class="event-card-header">
          <span class="event-date">${evt.acq_date}</span>
          <span class="event-risk-badge badge-${risk}">${risk}</span>
        </div>
        ${
          place
            ? `<div class="event-place">${place}</div>`
            : ''
        }
        <div class="event-details">
          <span>Lat: ${evt.latitude.toFixed(2)}°, Lon: ${evt.longitude.toFixed(2)}°</span>
          <span>${evt.brightness ? evt.brightness.toFixed(0) + ' K' : '-'} · Score: ${(evt.risk_score || 0).toFixed(2)}</span>
        </div>
      `;
      card.addEventListener('click', () => {
        this.selectEvent(evt);
        this.map.flyTo([evt.latitude, evt.longitude], 10, { duration: 1.2 });
      });
      container.appendChild(card);
    });
  }

  selectEvent(evt) {
    this.selectedEvent = evt;
    this.highlightSelectedInList();
    this.renderInspector(evt);
  }

  highlightSelectedInList() {
    document.querySelectorAll('.event-card').forEach((el, idx) => {
      if (this.events[idx] === this.selectedEvent) {
        el.classList.add('selected');
        el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      } else {
        el.classList.remove('selected');
      }
    });
  }

  renderInspector(evt) {
    const panel = document.getElementById('inspector-panel');
    const body = document.getElementById('inspector-content');
    const risk = (evt.risk_level || 'low').toLowerCase();
    const f = evt.risk_factors || {};
    const place = this.formatLocality(evt);

    body.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <span class="event-risk-badge badge-${risk}" style="font-size:13px;padding:4px 12px;">${risk.toUpperCase()}</span>
        <span style="font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:700;color:${RISK_COLORS[risk]};">
          Score: ${(evt.risk_score || 0).toFixed(3)}
        </span>
      </div>
      <div class="inspector-grid">
        <div class="inspector-cell" style="grid-column:1/-1;">
          <div class="inspector-cell-label">Localidad</div>
          <div class="inspector-cell-val">${
            place || '<span style="opacity:.55;">sin resolver</span>'
          }</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Latitud / Longitud</div>
          <div class="inspector-cell-val">${evt.latitude.toFixed(4)}, ${evt.longitude.toFixed(4)}</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Fecha Adquisición</div>
          <div class="inspector-cell-val">${evt.acq_date}</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Temperatura Anomalía</div>
          <div class="inspector-cell-val">${evt.brightness ? evt.brightness.toFixed(1) + ' K' : 'N/D'}</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Humedad Combustible (EMC)</div>
          <div class="inspector-cell-val" style="color:${f.fuel_moisture_pct < 10 ? '#ef4444' : '#10b981'};">
            ${f.fuel_moisture_pct != null ? f.fuel_moisture_pct + '%' : 'N/D'}
          </div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Temp. Ambiente (2m)</div>
          <div class="inspector-cell-val">${f.temperature_c != null ? f.temperature_c + ' °C' : 'N/D'}</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Humedad Relativa (2m)</div>
          <div class="inspector-cell-val">${f.relative_humidity != null ? f.relative_humidity + ' %' : 'N/D'}</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Viento (10m)</div>
          <div class="inspector-cell-val">${f.wind_speed_kmh != null ? f.wind_speed_kmh + ' km/h' : 'N/D'}</div>
        </div>
        <div class="inspector-cell">
          <div class="inspector-cell-label">Índice Propagación</div>
          <div class="inspector-cell-val">${f.spread_index != null ? f.spread_index : 'N/D'}</div>
        </div>
      </div>
    `;
    panel.style.display = 'block';
  }

}

// Cualquier 401 significa que la sesión expiró: redirige al formulario de login
// una sola vez, en vez de dejar el dashboard lleno de peticiones fallidas.
(function installAuthRedirect() {
  const nativeFetch = window.fetch.bind(window);
  let redirecting = false;

  window.fetch = async (...args) => {
    const res = await nativeFetch(...args);
    if (res.status === 401 && !redirecting) {
      const url = String((args[0] && args[0].url) || args[0] || '');
      if (!url.includes('/auth/')) {
        redirecting = true;
        location.replace('/login');
      }
    }
    return res;
  };
})();

document.addEventListener('DOMContentLoaded', () => {
  // Una sesión caducada debe redirigir al usuario al login, no renderizar
  // un dashboard vacío con peticiones erróneas.
  fetch('/auth/session')
    .then((r) => r.json())
    .then((d) => {
      if (!d.authenticated) location.replace('/login');
      else new FireWatchApp();
    })
    .catch(() => location.replace('/login'));
});