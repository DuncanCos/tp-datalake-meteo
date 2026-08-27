/* La Météo Grisaille — frontend */

const WMO_EMOJI = [
  [0, "☀️"], [1, "🌤️"], [2, "⛅"], [3, "☁️"], [45, "🌫️"], [48, "🌫️"],
  [51, "🌦️"], [53, "🌦️"], [55, "🌧️"], [61, "🌧️"], [63, "🌧️"], [65, "🌧️"],
  [71, "🌨️"], [73, "🌨️"], [75, "❄️"], [80, "🌧️"], [81, "🌧️"], [82, "⛈️"],
  [95, "⛈️"], [96, "⛈️"], [99, "⛈️"],
];
function emojiFor(code) {
  let best = "🌡️";
  for (const [c, e] of WMO_EMOJI) if (code >= c) best = e;
  return best;
}
// pas de code WMO dans l'historique quotidien : petite heuristique
function emojiForDaily(v) {
  if (v.precip_mm >= 10) return "🌧️";
  if (v.precip_mm >= 1) return "🌦️";
  if (v.temp_min != null && v.temp_min <= 0) return "❄️";
  if (v.sunshine_min != null && v.sunshine_min >= 300) return "☀️";
  if (v.sunshine_min != null && v.sunshine_min >= 120) return "⛅";
  return "☁️";
}

function meterColor(score) {
  if (score < 25) return "#0ca30c";
  if (score < 45) return "#fab219";
  if (score < 65) return "#ec835a";
  return "#6b4fae";
}

function mood(score) {
  if (score < 20) return "☀️ tout va bien !";
  if (score < 40) return "🌤️ ça passe";
  if (score < 60) return "😶‍🌫️ bof bof";
  if (score < 80) return "🌧️ sortez les plaids";
  return "🫠 misère totale";
}

/* ── carte de France ───────────────────────────────────── */
const CITY_COORDS = {
  Paris: [48.8566, 2.3522], Lyon: [45.764, 4.8357], Marseille: [43.2965, 5.3698],
  Toulouse: [43.6047, 1.4442], Nice: [43.7102, 7.262], Nantes: [47.2184, -1.5536],
  Strasbourg: [48.5734, 7.7521], Bordeaux: [44.8378, -0.5792],
  Lille: [50.6292, 3.0573], Rennes: [48.1173, -1.6778],
};
let map = null;
const markers = {};
let heatOverlay = null;

// etat temporel : "live" ou une date de l'historique
let mode = "live";
let lastLive = [];
let dayData = [];
let allDates = [];

function currentData() { return mode === "live" ? lastLive : dayData; }

/* couches heatmap : getLive / getDaily (null = pas dispo dans ce mode) */
const HEAT_LAYERS = {
  temp: { label: "🌡️ Chaleur", unit: "°C", colors: ["#2a78d6", "#fff3c4", "#d03b3b"],
    getLive: (v) => v.temperature, getDaily: (v) => v.temp_avg },
  vent: { label: "💨 Vent", unit: "km/h", colors: ["#f0fbf9", "#0b7285"],
    getLive: (v) => v.wind_speed_ms * 3.6, getDaily: (v) => v.wind_ms * 3.6 },
  humidite: { label: "💧 Humidité", unit: "%", colors: ["#f5faff", "#1c5cab"],
    getLive: (v) => v.humidity_pct, getDaily: (v) => v.humidity_pct },
  pluie: { label: "☔ Pluie", unit: "mm", liveUnit: " mm/h", colors: ["#f5faff", "#256abf"],
    getLive: (v) => v.precipitation_mm, getDaily: (v) => v.precip_mm },
  soleil: { label: "☀️ Soleil", unit: "min", liveUnit: " %", gridUnit: " J/cm²",
    colors: ["#8a8f99", "#ffd75e"],
    // en direct : pas d'héliomètre -> part de ciel dégagé (inverse nébulosité)
    getLive: (v) => 100 - v.cloud_cover_pct, getDaily: (v) => v.sunshine_min },
  nuages: { label: "☁️ Nuages", unit: "%", colors: ["#ffffff", "#5a6b7d"],
    getLive: (v) => v.cloud_cover_pct, getDaily: null },
  grisaille: { label: "🫠 Grisaille", unit: "/100", colors: ["#eaf6ea", "#4a3aa7"],
    getLive: (v) => v.grisaille_live, getDaily: (v) => v.grisaille },
};
let currentHeat = "temp";

/* grille SIM2 Météo-France (mailles SAFRAN agrégées à 16 km) : quand la
   journée affichée est couverte, elle remplace l'interpolation IDW.
   cells = [lat, lon, temp, precip mm, vent m/s, humidité %, ssi J/cm², grisaille] */
let gridCells = [];
let gridMode = null; // "daily" (indice complet) | "monthly" (partiel) | null

const GRID_IDX = { temp: 2, pluie: 3, vent: 4, humidite: 5, soleil: 6, grisaille: 7 };
const GRID_MONTHLY_KEYS = ["temp", "pluie", "grisaille"];

function gridGet(layerKey) {
  const idx = GRID_IDX[layerKey];
  if (idx == null) return null;
  if (gridMode === "monthly" && !GRID_MONTHLY_KEYS.includes(layerKey)) return null;
  const conv = layerKey === "vent" ? 3.6 : 1; // m/s -> km/h
  return (c) => (c[idx] == null ? null : c[idx] * conv);
}

// emprise France metropolitaine
const HEAT_BOUNDS = { south: 41.2, north: 51.4, west: -5.6, east: 10.0 };

/* contour France (metropole simplifiée, servie en local) : masque le reste
   du monde sur la carte et découpe les heatmaps aux frontières */
let francePolys = []; // liste d'anneaux [[lon, lat], ...]

async function loadFrance() {
  try {
    const geo = await (await fetch("france.geojson")).json();
    const geom = geo.type === "Feature" ? geo.geometry : geo.features[0].geometry;
    const polys = geom.type === "Polygon" ? [geom.coordinates] : geom.coordinates;
    francePolys = polys.flat(); // tous les anneaux (continent, Corse, îles)
    if (map) addFranceDecor();
    drawHeatOverlay(); // re-découpe l'overlay déjà affiché
  } catch { /* pas de contour -> carte non masquée, tout marche quand même */ }
}

function addFranceDecor() {
  if (!francePolys.length || map._franceDecor) return;
  map._franceDecor = true;
  const rings = francePolys.map((ring) => ring.map(([lon, lat]) => [lat, lon]));
  // monde entier avec la France en trous -> l'étranger est estompé
  const world = [[70, -30], [70, 30], [25, 30], [25, -30]];
  L.polygon([world, ...rings], {
    stroke: false, fillColor: "#dfeefc", fillOpacity: 0.78, interactive: false,
  }).addTo(map);
  L.polygon(rings, {
    color: "#14355c", weight: 1.5, opacity: 0.55, fill: false, interactive: false,
  }).addTo(map);
}

function clipToFrance(ctx, W, H) {
  if (!francePolys.length) return;
  const yMercF = (lat) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360));
  const yN = yMercF(HEAT_BOUNDS.north), yS = yMercF(HEAT_BOUNDS.south);
  ctx.globalCompositeOperation = "destination-in";
  ctx.beginPath();
  for (const ring of francePolys) {
    ring.forEach(([lon, lat], i) => {
      const x = ((lon - HEAT_BOUNDS.west) / (HEAT_BOUNDS.east - HEAT_BOUNDS.west)) * W;
      const y = ((yMercF(lat) - yN) / (yS - yN)) * H;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.closePath();
  }
  ctx.fill();
  ctx.globalCompositeOperation = "source-over";
}

function lerpColor(colors, t) {
  const n = colors.length - 1;
  const seg = Math.min(Math.floor(t * n), n - 1);
  const f = t * n - seg;
  const hex = (c) => [1, 3, 5].map((i) => parseInt(c.slice(i, i + 2), 16));
  const [a, b] = [hex(colors[seg]), hex(colors[seg + 1])];
  return a.map((x, i) => Math.round(x + (b[i] - x) * f));
}

function drawHeatOverlay() {
  // journée couverte par la grille SIM2 -> vraies mailles, pas d'IDW
  if (mode !== "live" && gridCells.length) { drawGridOverlay(); return; }
  const data = currentData();
  if (!map || !data.length) return;
  const layer = HEAT_LAYERS[currentHeat];
  const get = mode === "live" ? layer.getLive : layer.getDaily;
  if (!get) return;
  const pts = data
    .map((v) => ({ lat: CITY_COORDS[v.city]?.[0], lon: CITY_COORDS[v.city]?.[1],
                   val: get(v) }))
    .filter((p) => p.lat != null && p.val != null && !Number.isNaN(p.val));
  if (!pts.length) return;

  // echelle dynamique calee sur les valeurs du moment
  const vals = pts.map((p) => p.val);
  let vmin = Math.min(...vals), vmax = Math.max(...vals);
  const pad = Math.max((vmax - vmin) * 0.15, 0.5);
  const positive = Math.min(...vals) >= 0;
  vmin -= pad; vmax += pad;
  if (positive) vmin = Math.max(0, vmin);  // pas de -3 mm de pluie

  const W = 240, H = 300;
  const canvas = document.createElement("canvas");
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext("2d");
  const img = ctx.createImageData(W, H);

  // lignes espacees en Mercator pour caler l'image sur la carte
  const yMerc = (lat) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360));
  const yN = yMerc(HEAT_BOUNDS.north), yS = yMerc(HEAT_BOUNDS.south);
  const COSLAT = Math.cos((46.5 * Math.PI) / 180);

  for (let row = 0; row < H; row++) {
    const yM = yN + (row / (H - 1)) * (yS - yN);
    const lat = (Math.atan(Math.exp(yM)) * 360) / Math.PI - 90;
    for (let col = 0; col < W; col++) {
      const lon = HEAT_BOUNDS.west + (col / (W - 1)) * (HEAT_BOUNDS.east - HEAT_BOUNDS.west);
      // interpolation inverse-distance (IDW, p=2.5)
      let num = 0, den = 0;
      for (const p of pts) {
        const d = Math.hypot(lat - p.lat, (lon - p.lon) * COSLAT) + 0.05;
        const w = 1 / Math.pow(d, 2.5);
        num += w * p.val; den += w;
      }
      const t = Math.max(0, Math.min(1, (num / den - vmin) / (vmax - vmin)));
      const [r, g, b] = lerpColor(layer.colors, t);
      const i = (row * W + col) * 4;
      img.data[i] = r; img.data[i + 1] = g; img.data[i + 2] = b;
      img.data[i + 3] = 150;
    }
  }
  ctx.putImageData(img, 0, 0);
  clipToFrance(ctx, W, H);

  const bounds = [[HEAT_BOUNDS.south, HEAT_BOUNDS.west], [HEAT_BOUNDS.north, HEAT_BOUNDS.east]];
  const url = canvas.toDataURL();
  if (heatOverlay) heatOverlay.setUrl(url);
  else heatOverlay = L.imageOverlay(url, bounds, { opacity: 0.85, interactive: false }).addTo(map);

  const unit = mode === "live" ? (layer.liveUnit ?? layer.unit) : layer.unit;
  document.getElementById("map-legend").innerHTML = `
    <div>${layer.label}</div>
    <div class="bar" style="background:linear-gradient(90deg,${layer.colors.join(",")})"></div>
    <div class="ends"><span>${vmin.toFixed(0)}${unit}</span><span>${vmax.toFixed(0)}${unit}</span></div>
    <div class="src">interpolation 10 villes</div>`;
}

function drawGridOverlay() {
  if (!map) return;
  const layer = HEAT_LAYERS[currentHeat];
  const get = gridGet(currentHeat);
  if (!get) return;
  const pts = gridCells
    .map((c) => ({ lat: c[0], lon: c[1], val: get(c) }))
    .filter((p) => p.lat != null && p.val != null && !Number.isNaN(p.val));
  if (!pts.length) return;

  const vals = pts.map((p) => p.val);
  let vmin = Math.min(...vals), vmax = Math.max(...vals);
  const pad = Math.max((vmax - vmin) * 0.05, 0.5);
  const positive = Math.min(...vals) >= 0;
  vmin -= pad; vmax += pad;
  if (positive) vmin = Math.max(0, vmin);

  // résolution doublée par rapport à l'IDW : les cellules 16 km sont nettes
  const W = 480, H = 600;
  const canvas = document.createElement("canvas");
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext("2d");

  const yMerc = (lat) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360));
  const yN = yMerc(HEAT_BOUNDS.north), yS = yMerc(HEAT_BOUNDS.south);
  const px = (lon) => ((lon - HEAT_BOUNDS.west) / (HEAT_BOUNDS.east - HEAT_BOUNDS.west)) * W;
  const py = (lat) => ((yMerc(lat) - yN) / (yS - yN)) * H;

  const DLAT = 0.144; // ~16 km en degrés de latitude
  for (const p of pts) {
    const t = Math.max(0, Math.min(1, (p.val - vmin) / (vmax - vmin)));
    const [r, g, b] = lerpColor(layer.colors, t);
    ctx.fillStyle = `rgba(${r},${g},${b},0.62)`;
    const dLon = DLAT / Math.cos((p.lat * Math.PI) / 180);
    const x0 = px(p.lon - dLon / 2), x1 = px(p.lon + dLon / 2);
    const y0 = py(p.lat + DLAT / 2), y1 = py(p.lat - DLAT / 2);
    ctx.fillRect(x0, y0, Math.ceil(x1 - x0) + 1, Math.ceil(y1 - y0) + 1);
  }
  clipToFrance(ctx, W, H);

  const bounds = [[HEAT_BOUNDS.south, HEAT_BOUNDS.west], [HEAT_BOUNDS.north, HEAT_BOUNDS.east]];
  const url = canvas.toDataURL();
  if (heatOverlay) heatOverlay.setUrl(url);
  else heatOverlay = L.imageOverlay(url, bounds, { opacity: 0.85, interactive: false }).addTo(map);

  const unit = layer.gridUnit ?? layer.unit;
  const label = gridMode === "monthly" && currentHeat === "grisaille"
    ? "🫠 Grisaille partielle" : layer.label;
  document.getElementById("map-legend").innerHTML = `
    <div>${label}</div>
    <div class="bar" style="background:linear-gradient(90deg,${layer.colors.join(",")})"></div>
    <div class="ends"><span>${vmin.toFixed(0)}${unit}</span><span>${vmax.toFixed(0)}${unit}</span></div>
    <div class="src">grille SIM2 Météo-France · 16 km</div>`;
}

function renderLayerChips() {
  const el = document.getElementById("layer-chips");
  const avail = Object.entries(HEAT_LAYERS)
    .filter(([k, l]) => {
      if (mode === "live") return l.getLive;
      if (gridCells.length) return gridGet(k);
      return l.getDaily;
    });
  if (!avail.some(([k]) => k === currentHeat)) currentHeat = "temp";
  el.innerHTML = avail.map(([k, l]) =>
    `<button class="layer-chip${k === currentHeat ? " active" : ""}" data-layer="${k}">${l.label}</button>`
  ).join("");
  el.querySelectorAll(".layer-chip").forEach((btn) => btn.addEventListener("click", () => {
    currentHeat = btn.dataset.layer;
    el.querySelectorAll(".layer-chip").forEach((b) => b.classList.toggle("active", b === btn));
    drawHeatOverlay();
  }));
}

function pinAndPopup(v) {
  if (mode === "live") {
    const score = v.grisaille_live, color = meterColor(score);
    return {
      emoji: emojiFor(v.weather_code), color, score,
      pill: `${v.city} ${v.temperature.toFixed(0)}° · ${score.toFixed(0)}`,
      popup: `<div class="popup-title">${emojiFor(v.weather_code)} ${v.city}
          — #${v.rang_misere_live} misère</div>
        🌡️ ${v.temperature.toFixed(1)}°C · 💨 ${(v.wind_speed_ms * 3.6).toFixed(0)} km/h<br>
        ☁️ ${v.cloud_cover_pct.toFixed(0)} % · 💧 ${v.humidity_pct.toFixed(0)} %
          · ☔ ${v.precipitation_mm.toFixed(1)} mm<br>
        <b style="color:${color}">grisaille ${score.toFixed(0)}/100 — ${mood(score)}</b>
        <div class="popup-meter"><div style="width:${score}%;background:${color}"></div></div>`,
    };
  }
  const score = v.grisaille ?? 0, color = meterColor(score);
  const fmt = (x, f = 0) => (x == null ? "–" : x.toFixed(f));
  return {
    emoji: emojiForDaily(v), color, score,
    pill: `${v.city} ${fmt(v.temp_avg)}° · ${fmt(score)}`,
    popup: `<div class="popup-title">${emojiForDaily(v)} ${v.city} — ${v.obs_date}</div>
      🌡️ ${fmt(v.temp_min, 1)} / ${fmt(v.temp_avg, 1)} / ${fmt(v.temp_max, 1)}°C<br>
      ☔ ${fmt(v.precip_mm, 1)} mm · 💨 ${fmt(v.wind_ms == null ? null : v.wind_ms * 3.6)} km/h
        (raf. ${fmt(v.gust_ms == null ? null : v.gust_ms * 3.6)})<br>
      ☀️ ${fmt(v.sunshine_min)} min de soleil · 💧 ${fmt(v.humidity_pct)} %
        · 📡 ${v.n_stations} stations<br>
      <b style="color:${color}">grisaille ${fmt(score)}/100 — ${mood(score)}</b>
      <div class="popup-meter"><div style="width:${score}%;background:${color}"></div></div>`,
  };
}

function updateMap() {
  if (!map) initMap();
  const data = currentData();
  const seen = new Set();
  for (const v of data) {
    const coords = CITY_COORDS[v.city];
    if (!coords) continue;
    seen.add(v.city);
    const { emoji, color, pill, popup } = pinAndPopup(v);
    const icon = L.divIcon({
      className: "city-marker",
      iconSize: [90, 56],
      iconAnchor: [45, 46],
      html: `<div class="pin" style="color:${color}">
        <span class="wemoji">${emoji}</span><span class="pill">${pill}</span></div>`,
    });
    if (markers[v.city]) markers[v.city].setIcon(icon).setPopupContent(popup);
    else markers[v.city] = L.marker(coords, { icon }).addTo(map).bindPopup(popup);
  }
  for (const [city, m] of Object.entries(markers)) {
    if (!seen.has(city)) { m.remove(); delete markers[city]; }
  }
}

function initMap() {
  map = L.map("map", {
    scrollWheelZoom: false,
    // la carte reste cadrée sur la métropole : pas de pan vers l'étranger
    maxBounds: [[40.6, -7.0], [52.0, 11.0]],
    maxBoundsViscosity: 1.0,
    minZoom: 5,
  }).setView([46.6, 2.6], 6);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap",
    maxZoom: 12,
  }).addTo(map);
  addFranceDecor();
}

/* ── controles temporels ───────────────────────────────── */
// le slider couvre UNE annee (du 1er jour au dernier jour disponibles),
// l'annee se choisit dans le menu deroulant
let currentYear = "";
let yearDates = [];

function datesForYear(y) { return allDates.filter((d) => d.startsWith(y)); }

function setYear(y) {
  currentYear = y;
  yearDates = datesForYear(y);
  const pick = document.getElementById("day-pick");
  const slider = document.getElementById("day-slider");
  document.getElementById("year-pick").value = y;
  pick.min = yearDates[0];
  pick.max = yearDates[yearDates.length - 1];
  slider.max = yearDates.length - 1;
}

async function setDay(date) {
  const y = date.slice(0, 4);
  if (y !== currentYear) setYear(y);
  mode = "date";
  dayData = await (await fetch(`/api/day?date=${date}`)).json();
  // grille SIM2 : quotidienne (fenêtre ~60 j), sinon mensuelle, sinon IDW
  gridCells = []; gridMode = null;
  try {
    const g = await (await fetch(`/api/grid?date=${date}`)).json();
    if (g.cells?.length) { gridCells = g.cells; gridMode = "daily"; }
    else {
      const m = parseInt(date.slice(5, 7), 10);
      const gm = await (await fetch(`/api/grid_month?year=${y}&month=${m}`)).json();
      if (gm.cells?.length) { gridCells = gm.cells; gridMode = "monthly"; }
    }
  } catch { /* pas de grille -> l'IDW 10 villes prend le relais */ }
  document.getElementById("btn-live").classList.remove("active");
  document.getElementById("day-pick").value = date;
  document.getElementById("day-slider").value = yearDates.indexOf(date);
  document.getElementById("live-time").textContent = `· journée du ${date}`;
  renderLayerChips();
  updateMap();
  drawHeatOverlay();
}

let liveUpdatedAt = 0;

function liveLabel() {
  if (!lastLive.length) return "";
  let label = "· relevé modèle " + lastLive[0].obs_time.slice(11, 16) + " UTC";
  if (liveUpdatedAt) {
    const min = Math.round((Date.now() - liveUpdatedAt) / 60000);
    const badge = min <= 20 ? "🟢" : min <= 90 ? "🟠" : "🔴";
    const ago = min < 1 ? "à l'instant" : min < 60 ? `il y a ${min} min`
      : `il y a ${Math.floor(min / 60)} h ${String(min % 60).padStart(2, "0")}`;
    label += ` · pipeline mis à jour ${ago} ${badge}`;
  }
  return label;
}

function setLive() {
  mode = "live";
  document.getElementById("btn-live").classList.add("active");
  document.getElementById("live-time").textContent = liveLabel();
  renderLayerChips();
  updateMap();
  drawHeatOverlay();
}

async function initTimeControls() {
  allDates = await (await fetch("/api/dates")).json();
  if (!allDates.length) return;
  const pick = document.getElementById("day-pick");
  const slider = document.getElementById("day-slider");
  const yearSel = document.getElementById("year-pick");

  const years = [...new Set(allDates.map((d) => d.slice(0, 4)))];
  yearSel.innerHTML = years.map((y) => `<option value="${y}">${y}</option>`).join("");
  setYear(years[years.length - 1]);
  slider.value = yearDates.length - 1;

  document.getElementById("btn-live").addEventListener("click", setLive);
  yearSel.addEventListener("change", () => {
    const y = yearSel.value;
    setYear(y);
    setDay(yearDates[0]); // on atterrit au 1er jour de l'annee choisie
  });
  pick.addEventListener("change", () => {
    if (allDates.includes(pick.value)) setDay(pick.value);
  });
  // debounce : pas une requete par pixel pendant le glissement
  let sliderTimer = null;
  slider.addEventListener("input", () => {
    const date = yearDates[slider.value];
    document.getElementById("live-time").textContent = `· journée du ${date}`;
    clearTimeout(sliderTimer);
    sliderTimer = setTimeout(() => setDay(date), 150);
  });
  document.getElementById("day-prev").addEventListener("click", () => {
    const i = mode === "live" ? allDates.length - 1 : allDates.indexOf(pick.value) - 1;
    if (i >= 0) setDay(allDates[i]);
  });
  document.getElementById("day-next").addEventListener("click", () => {
    if (mode === "live") return;
    const i = allDates.indexOf(pick.value) + 1;
    if (i < allDates.length) setDay(allDates[i]);
    else setLive();
  });
}

/* ── en direct ─────────────────────────────────────────── */
async function loadLive() {
  const payload = await (await fetch("/api/live")).json();
  const data = payload.villes || [];
  if (!data.length) return;
  lastLive = data;
  liveUpdatedAt = payload.updated_at || 0;
  if (mode === "live") {
    document.getElementById("live-time").textContent = liveLabel();
    updateMap();
    drawHeatOverlay();
  }
  document.getElementById("live-cards").innerHTML = data.map((v) => `
    <div class="card">
      <div class="ville">${v.city}
        <span class="badge-rank">#${v.rang_misere_live} misère</span></div>
      <div class="emoji">${emojiFor(v.weather_code)}</div>
      <div class="temp">${v.temperature.toFixed(1)}°C</div>
      <div class="detail">💨 ${(v.wind_speed_ms * 3.6).toFixed(0)} km/h ·
        ☁️ ${v.cloud_cover_pct.toFixed(0)} % · 💧 ${v.humidity_pct.toFixed(0)} %</div>
      <div class="meter"><div style="width:${v.grisaille_live}%;
        background:${meterColor(v.grisaille_live)}"></div></div>
      <div class="score" style="color:${meterColor(v.grisaille_live)}">
        grisaille ${v.grisaille_live.toFixed(0)}/100 — ${mood(v.grisaille_live)}</div>
    </div>`).join("");
}

/* ── podium ────────────────────────────────────────────── */
const MOIS = ["", "janvier", "février", "mars", "avril", "mai", "juin", "juillet",
  "août", "septembre", "octobre", "novembre", "décembre"];

const PODIUM_TYPES = {
  mois: "📅 Le mois",
  annee: "📆 L'année",
  "5ans": "🕔 Sur 5 ans",
  "10ans": "🕙 Sur 10 ans",
};
let podiumsData = {};
let currentPodium = "mois";

function podiumLabel(ptype, label) {
  if (ptype === "mois") {
    const [y, m] = label.split("-");
    return `· ${MOIS[parseInt(m, 10)]} ${y}`;
  }
  return `· ${label}`;
}

function renderPodium() {
  const data = podiumsData[currentPodium];
  if (!data) return;
  const villes = data.villes;
  document.getElementById("ranking-month").textContent =
    podiumLabel(currentPodium, data.label);
  const medailles = ["🥇", "🥈", "🥉"];
  const ordre = [1, 0, 2]; // 2e, 1er, 3e pour un vrai podium
  const cls = ["p2", "p1", "p3"];
  document.getElementById("podium").innerHTML = ordre.map((i, k) => {
    const v = villes[i];
    if (!v) return "";
    // au mois : cumul de pluie ; sur une periode longue : moyenne annuelle
    const pluie = currentPodium === "mois"
      ? `☔ ${v.precip_cumul_mm} mm`
      : `☔ ${Math.round(v.precip_cumul_mm / Math.max(v.n_jours / 365.25, 1e-6))} mm/an`;
    return `<div class="step ${cls[k]}"><div class="box">
      <div class="medaille">${medailles[i]}</div>
      <div class="nom">${v.city}</div>
      <div class="sc">${v.grisaille_moy}/100</div>
      <div class="detail" style="font-size:.75rem">🌡️ ${v.temp_moy}°C · ${pluie}</div>
    </div><div class="socle">${v.rang_misere === 1 ? "1ᵉʳ" : v.rang_misere + "ᵉ"}</div></div>`;
  }).join("");
  document.getElementById("ranking-rest").innerHTML = villes.slice(3).map((v) =>
    `<span class="chip">#${v.rang_misere} ${v.city} — ${v.grisaille_moy}</span>`
  ).join("");
}

async function loadRanking() {
  podiumsData = await (await fetch("/api/podiums")).json();
  const avail = Object.keys(PODIUM_TYPES).filter((t) => podiumsData[t]);
  if (!avail.length) return;
  if (!avail.includes(currentPodium)) currentPodium = avail[0];
  const el = document.getElementById("podium-chips");
  el.innerHTML = avail.map((t) =>
    `<button class="layer-chip${t === currentPodium ? " active" : ""}"
      data-podium="${t}">${PODIUM_TYPES[t]}</button>`).join("");
  el.querySelectorAll(".layer-chip").forEach((btn) => btn.addEventListener("click", () => {
    currentPodium = btn.dataset.podium;
    el.querySelectorAll(".layer-chip").forEach((b) => b.classList.toggle("active", b === btn));
    renderPodium();
  }));
  renderPodium();
}

/* ── courbe 30 jours ───────────────────────────────────── */
// 4 villes max, couleurs fixes par ville (palette validée accessibilité)
const CHART_CITIES = {
  Strasbourg: "#4a3aa7",
  Lille: "#2a78d6",
  Paris: "#eb6834",
  Nice: "#eda100",
};

async function loadChart() {
  const data = await (await fetch("/api/daily?days=30")).json();
  if (!data.length) return;
  const dates = [...new Set(data.map((r) => r.obs_date.slice(0, 10)))].sort();
  const datasets = Object.entries(CHART_CITIES).map(([city, color]) => {
    const byDate = Object.fromEntries(
      data.filter((r) => r.city === city)
          .map((r) => [r.obs_date.slice(0, 10), r.grisaille]));
    return {
      label: city,
      data: dates.map((d) => byDate[d] ?? null),
      borderColor: color,
      backgroundColor: color,
      borderWidth: 2,
      pointRadius: 2,
      pointHoverRadius: 5,
      tension: 0.3,
      spanGaps: true,
    };
  });
  new Chart(document.getElementById("chart"), {
    type: "line",
    data: { labels: dates.map((d) => d.slice(5)), datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { min: 0, max: 100,
             title: { display: true, text: "indice grisaille" },
             grid: { color: "#eceae4" } },
        x: { grid: { display: false } },
      },
      plugins: {
        legend: { position: "top", labels: { font: { family: "'Baloo 2'", weight: 700 } } },
        tooltip: { backgroundColor: "#14355c" },
      },
    },
  });
}

/* ── fiabilité du live (live vs officiel) ──────────────── */
function reliabilityBadge(mae) {
  if (mae < 0.5) return { ico: "🟢", txt: "fiable" };
  if (mae < 1.5) return { ico: "🟠", txt: "correct" };
  return { ico: "🔴", txt: "douteux" };
}

async function loadReliability() {
  const rel = await (await fetch("/api/reliability")).json();
  if (!rel.length) return; // tables pas encore construites : section masquée
  const hist = await (await fetch("/api/live_vs_official?days=14")).json();
  document.getElementById("reliability-section").style.display = "";
  document.getElementById("reliability-cards").innerHTML = rel.map((v) => {
    const b = reliabilityBadge(v.mae_temp);
    return `<div class="card">
      <div class="ville">${v.city}
        <span class="badge-rank">${b.ico} ${b.txt}</span></div>
      <div class="detail">🌡️ écart moyen ${v.mae_temp}°C
        (biais ${v.biais_temp > 0 ? "+" : ""}${v.biais_temp}°C)<br>
        ☔ écart moyen ${v.mae_precip ?? "–"} mm ·
        📅 ${v.n_jours_compares} j comparés</div>
      <div class="rel-chart"><canvas id="rel-${v.city}"></canvas></div>
    </div>`;
  }).join("");
  for (const v of rel) {
    const rows = hist.filter((r) => r.city === v.city);
    if (!rows.length) continue;
    new Chart(document.getElementById(`rel-${v.city}`), {
      type: "bar",
      data: {
        labels: rows.map((r) => r.obs_day.slice(5, 10)),
        datasets: [{
          data: rows.map((r) => r.ecart_temp),
          backgroundColor: rows.map((r) =>
            Math.abs(r.ecart_temp ?? 0) < 1 ? "#1baf7a" : "#eb6834"),
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: {
            label: (c) => `écart ${c.raw > 0 ? "+" : ""}${c.raw}°C` } },
        },
        scales: {
          y: { ticks: { font: { size: 9 } }, grid: { color: "#eceae4" } },
          x: { display: false },
        },
      },
    });
  }
}

/* ── épisodes ──────────────────────────────────────────── */
const EP_STYLE = {
  vague_de_froid: { cls: "froid", ico: "🥶", nom: "Vague de froid",
    det: (e) => `min ${e.intensite}°C` },
  episode_pluvieux: { cls: "pluie", ico: "🌧️", nom: "Épisode pluvieux",
    det: (e) => `${e.intensite} mm cumulés` },
  coup_de_vent: { cls: "vent", ico: "💨", nom: "Coup de vent",
    det: (e) => `rafales ${(e.intensite * 3.6).toFixed(0)} km/h` },
};

// croisement avec les archives de vigilance Meteo-France (si disponible)
function vigBadge(e) {
  if (e.statut === "confirme") {
    const taux = Math.round((e.taux_recouvrement ?? 0) * 100);
    return `<span class="vig-badge ok">✔ vigilance ${e.couleur_max ?? ""} (${taux} %)</span>`;
  }
  if (e.statut === "non_confirme")
    return `<span class="vig-badge ko">✖ non vu par la vigilance</span>`;
  return ""; // hors_archive (avant fin 2022) ou table pas encore croisée
}

async function loadEpisodes() {
  const data = await (await fetch("/api/episodes?limit=12")).json();
  document.getElementById("episodes").innerHTML = data.map((e) => {
    const s = EP_STYLE[e.type];
    return `<div class="episode ${s.cls}"><span class="eico">${s.ico}</span>
      <span>${s.nom} à <b>${e.city}</b>
        <span class="edet">du ${e.date_debut} au ${e.date_fin}
        (${e.duree_jours} j) — ${s.det(e)}</span> ${vigBadge(e)}</span></div>`;
  }).join("");
}

renderLayerChips();
loadFrance();
loadLive();
initTimeControls();
loadRanking();
loadChart();
loadEpisodes();
loadReliability();
setInterval(loadLive, 60_000);
// le "il y a X min" vieillit meme sans nouvelle donnee
setInterval(() => {
  if (mode === "live")
    document.getElementById("live-time").textContent = liveLabel();
}, 30_000);
