let buses = Array.isArray(window.liveBuses) ? window.liveBuses : [];
let userLocation = null;
let userMarkerLayer = null;

const mapElement = document.getElementById('map');
const map = mapElement ? L.map('map').setView([15.37, 120.94], 10) : null;
if (map) {
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(map);
}

const busList = document.getElementById('busList');
const activeBusCount = document.getElementById('activeBusCount');
const avgCrowd = document.getElementById('avgCrowd');
const highCrowdCount = document.getElementById('highCrowdCount');
const crowdLevels = document.getElementById('crowdLevels');
const userLocationText = document.getElementById('userLocationText');
const sortSelect = document.getElementById('sortSelect');
const locateMeBtn = document.getElementById('locateMeBtn');
const menuToggle = document.getElementById('menuToggle');
const menuOverlay = document.getElementById('menuOverlay');
const menuClose = document.getElementById('menuClose');

let mapLayers = [];

function hasValidCoordinates(lat, lng) {
  return (
    lat !== null &&
    lat !== undefined &&
    lng !== null &&
    lng !== undefined &&
    Number.isFinite(Number(lat)) &&
    Number.isFinite(Number(lng)) &&
    Math.abs(Number(lat)) <= 90 &&
    Math.abs(Number(lng)) <= 180
  );
}

function markerColor(level) {
  if (level === 'High') return '#dc2626';
  if (level === 'Medium') return '#ea580c';
  return '#16a34a';
}

function createBusIcon(level) {
  const fill = markerColor(level);
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="42" height="42" viewBox="0 0 42 42">
      <g fill="none" fill-rule="evenodd">
        <path d="M21 3C11.6 3 6 6.8 6 13v12.5c0 2.2 1.8 4 4 4H11v4.2c0 1.2 1 2.3 2.3 2.3h1.4c1.2 0 2.3-1 2.3-2.3v-4.2h8v4.2c0 1.2 1 2.3 2.3 2.3h1.4c1.2 0 2.3-1 2.3-2.3v-4.2H32c2.2 0 4-1.8 4-4V13c0-6.2-5.6-10-15-10Z" fill="${fill}" stroke="#0f172a" stroke-width="1.5"/>
        <rect x="10" y="10" width="22" height="8" rx="2.5" fill="#e0f2fe" stroke="#0f172a" stroke-width="1.2"/>
        <path d="M10 22h22" stroke="#0f172a" stroke-width="1.4"/>
        <circle cx="14" cy="28" r="3.2" fill="#111827"/>
        <circle cx="28" cy="28" r="3.2" fill="#111827"/>
      </g>
    </svg>
  `;

  return L.icon({
    iconUrl: `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`,
    iconSize: [42, 42],
    iconAnchor: [21, 21],
    popupAnchor: [0, -18]
  });
}

function createUserIcon() {
  const svg = `
    <div class="user-marker-wrap">
      <svg class="user-marker-svg" xmlns="http://www.w3.org/2000/svg" width="44" height="44" viewBox="0 0 44 44">
        <circle cx="22" cy="22" r="18" fill="#1d4ed8" opacity="0.14"/>
        <g transform="translate(7 5)">
          <path d="M15 0C11.6 0 8.8 2.8 8.8 6.2S11.6 12.4 15 12.4s6.2-2.8 6.2-6.2S18.4 0 15 0Z" fill="#2563eb" stroke="#ffffff" stroke-width="1.6"/>
          <path d="M15 14.8c-6 0-10.8 4.7-10.8 10.6v3.1c0 1 0.8 1.9 1.9 1.9h17.8c1 0 1.9-0.8 1.9-1.9v-3.1c0-5.9-4.9-10.6-10.8-10.6Z" fill="#60a5fa" stroke="#ffffff" stroke-width="1.6"/>
          <circle cx="15" cy="22" r="1.8" fill="#ffffff"/>
          <path d="M15 24.4v4.8M10.8 30.4l4.2-3.4 4.2 3.4" stroke="#ffffff" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/>
        </g>
      </svg>
    </div>
  `;

  return L.divIcon({
    html: svg,
    className: 'user-marker-icon',
    iconSize: [44, 44],
    iconAnchor: [22, 22],
    popupAnchor: [0, -18]
  });
}

function focusUserMarker() {
  if (!userMarkerLayer || !userMarkerLayer._icon) {
    return;
  }

  const inner = userMarkerLayer._icon.querySelector('.user-marker-wrap');
  if (!inner) {
    return;
  }
  inner.classList.remove('user-marker-pop');
  void inner.offsetWidth;
  inner.classList.add('user-marker-pop');

  setTimeout(() => {
    if (inner) {
      inner.classList.remove('user-marker-pop');
    }
  }, 1400);
}

function clearMapLayers() {
  if (!map) {
    return;
  }
  mapLayers.forEach((layer) => map.removeLayer(layer));
  mapLayers = [];
}

function addLayer(layer) {
  if (!map) {
    return layer;
  }
  layer.addTo(map);
  mapLayers.push(layer);
  return layer;
}

function summarizeBuses(items) {
  const onlineBuses = items.filter((bus) => bus.status === 'online');
  const counts = { Low: 0, Medium: 0, High: 0 };
  let totalLoad = 0;

  onlineBuses.forEach((bus) => {
    const level = bus.crowdLevel === 'High' || bus.crowdLevel === 'Medium' ? bus.crowdLevel : 'Low';
    counts[level] += 1;
    totalLoad += Number(bus.capacityRatio) > 0
      ? Number(bus.capacityRatio) * 100
      : (Number(bus.capacity) > 0 ? (Number(bus.passengers) / Number(bus.capacity)) * 100 : 0);
  });

  return {
    active_bus_count: onlineBuses.length,
    avg_crowd: onlineBuses.length ? Math.round(totalLoad / onlineBuses.length) : 0,
    low_count: counts.Low,
    medium_count: counts.Medium,
    high_count: counts.High
  };
}

function distanceKm(a, b) {
  if (!a || !b) return Number.POSITIVE_INFINITY;
  const toRad = (value) => (value * Math.PI) / 180;
  const earth = 6371;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);

  const hav =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.sin(dLng / 2) * Math.sin(dLng / 2) * Math.cos(lat1) * Math.cos(lat2);

  return earth * 2 * Math.atan2(Math.sqrt(hav), Math.sqrt(1 - hav));
}

function offsetIfOverlapping(lat, lng) {
  if (!userLocation) {
    return [lat, lng];
  }

  const overlapDistance = distanceKm(userLocation, { lat, lng });
  if (overlapDistance > 0.03) {
    return [lat, lng];
  }

  return [lat + 0.00035, lng + 0.00035];
}

function getSortedBuses() {
  const sorted = buses.filter((bus) => bus.status === 'online');
  const sortMode = sortSelect ? sortSelect.value : 'nearest';

  sorted.forEach((bus) => {
    bus.distanceKm = userLocation && hasValidCoordinates(bus.lat, bus.lng)
      ? distanceKm(userLocation, { lat: Number(bus.lat), lng: Number(bus.lng) })
      : Number.POSITIVE_INFINITY;
    bus.capacityRatio = Number(bus.capacity) > 0 ? Number(bus.passengers) / Number(bus.capacity) : 0;
  });

  if (sortMode === 'capacity_low') {
    sorted.sort((a, b) => a.capacityRatio - b.capacityRatio);
  } else if (sortMode === 'capacity_high') {
    sorted.sort((a, b) => b.capacityRatio - a.capacityRatio);
  } else {
    sorted.sort((a, b) => a.distanceKm - b.distanceKm);
  }

  return sorted;
}

function renderBusList() {
  if (!busList) return;

  const sortedBuses = getSortedBuses();

  if (!sortedBuses.length) {
    busList.innerHTML = '<div class="bus-card"><h3>No buses found</h3><p>The system will show fleet units here once they are registered.</p></div>';
    return;
  }

  busList.innerHTML = sortedBuses.map((bus, index) => `
    <article class="bus-card ${index === 0 && sortSelect && sortSelect.value === 'nearest' ? 'nearest' : ''}" style="border-left-color:${bus.routeColor || '#1d4ed8'}">
      <h3>${bus.id}</h3>
      <p>${bus.direction}</p>
      <div class="row">
        <span>${bus.passengers}/${bus.capacity} passengers</span>
        <span class="crowd-pill ${bus.crowdLevel}">${bus.crowdLevel}</span>
      </div>
      <div class="row">
        <span>Status: ${bus.status}</span>
        <span>${bus.driver}</span>
      </div>
      <div class="row">
        <span>${bus.nextStop}</span>
        <span>${bus.tripStatus === 'active' && Number.isFinite(bus.distanceKm) ? `${bus.distanceKm.toFixed(2)} km away` : 'Distance unavailable'}</span>
      </div>
      <div class="row">
        <span>${Math.round(bus.capacityRatio * 100)}% load</span>
        <span>${bus.tripStatus === 'active' ? 'Live trip' : 'No active trip'}</span>
      </div>
    </article>
  `).join('');
}

function renderSummary(summary) {
  if (activeBusCount) activeBusCount.textContent = summary.active_bus_count ?? buses.length;
  if (avgCrowd) avgCrowd.textContent = summary.avg_crowd ?? 0;
  if (highCrowdCount) highCrowdCount.textContent = summary.high_count ?? 0;
  if (crowdLevels) {
    crowdLevels.innerHTML = `
      <div><span class="dot low"></span>Low ${summary.low_count ?? 0}</div>
      <div><span class="dot medium"></span>Medium ${summary.medium_count ?? 0}</div>
      <div><span class="dot high"></span>High ${summary.high_count ?? 0}</div>
    `;
  }
}

function renderMap() {
  if (!map) {
    return;
  }
  clearMapLayers();

  const sortedBuses = getSortedBuses();
  const bounds = [];

  if (userLocation) {
    userMarkerLayer = addLayer(L.marker([userLocation.lat, userLocation.lng], {
      icon: createUserIcon()
    }));
    userMarkerLayer.bindPopup('<strong>You are here</strong>');
    bounds.push([userLocation.lat, userLocation.lng]);
  }

  sortedBuses.forEach((bus) => {
    const hasLivePosition = hasValidCoordinates(bus.lat, bus.lng);
    const coords = Array.isArray(bus.coords) ? bus.coords : [];
    const history = Array.isArray(bus.history) ? bus.history : [];

    if (coords.length && bus.tripStatus === 'active') {
      const routeLine = addLayer(L.polyline(coords, {
        color: bus.routeColor || '#1d4ed8',
        weight: 5,
        opacity: 0.75
      }));
      routeLine.bindPopup(`${bus.direction}`);
      bounds.push(...coords);
    }

    if (history.length > 1) {
      const trail = addLayer(L.polyline(history, {
        color: bus.routeColor || '#1d4ed8',
        weight: 4,
        opacity: 0.95,
        dashArray: '8, 8'
      }));
      trail.bindPopup(`${bus.id} live travel history`);
      bounds.push(...history);
    }

    if (!hasLivePosition) {
      return;
    }

    const [markerLat, markerLng] = offsetIfOverlapping(Number(bus.lat), Number(bus.lng));
    const marker = addLayer(L.marker([markerLat, markerLng], {
      icon: createBusIcon(bus.crowdLevel)
    }));
    marker.bindPopup(`
      <strong>${bus.id}</strong><br>
      ${bus.direction}<br>
      Status: ${bus.status}<br>
      ${bus.passengers}/${bus.capacity} passengers<br>
      ${bus.nextStop}
    `);
    bounds.push([markerLat, markerLng]);
  });

  if (bounds.length) {
    map.fitBounds(bounds, { padding: [24, 24] });
  } else if (userLocation) {
    map.setView([userLocation.lat, userLocation.lng], 14);
  }
}

function updateUserLocationText(text) {
  if (userLocationText) {
    userLocationText.textContent = text;
  }
}

async function resolveLocationName(lat, lng) {
  try {
    const response = await fetch(`https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${lat}&lon=${lng}`, {
      headers: {
        'Accept-Language': 'en'
      }
    });
    if (!response.ok) {
      return null;
    }

    const data = await response.json();
    const address = data.address || {};
    return (
      address.city ||
      address.town ||
      address.municipality ||
      address.village ||
      address.county ||
      address.state ||
      null
    );
  } catch (error) {
    return null;
  }
}

function detectUserLocation() {
  if (!navigator.geolocation) {
    updateUserLocationText('Location is not available on this device');
    renderBusList();
    renderMap();
    return;
  }

  navigator.geolocation.getCurrentPosition(
    async (position) => {
      userLocation = {
        lat: position.coords.latitude,
        lng: position.coords.longitude
      };
      updateUserLocationText('Detecting your city...');
      const locationName = await resolveLocationName(userLocation.lat, userLocation.lng);
      updateUserLocationText(locationName || `${userLocation.lat.toFixed(5)}, ${userLocation.lng.toFixed(5)}`);
      renderBusList();
      renderMap();
      if (map) {
        map.setView([userLocation.lat, userLocation.lng], 15);
      }
      setTimeout(focusUserMarker, 120);
    },
    () => {
      updateUserLocationText('Location permission denied');
      renderBusList();
      renderMap();
    },
    {
      enableHighAccuracy: true,
      maximumAge: 10000,
      timeout: 10000
    }
  );
}

async function refreshLiveData() {
  try {
    const response = await fetch(window.liveBusEndpoint, { cache: 'no-store' });
    if (!response.ok) return;
    const payload = await response.json();
    buses = Array.isArray(payload.buses) ? payload.buses : [];
    renderSummary(payload);
    renderBusList();
    renderMap();
  } catch (error) {
    console.error('Live data refresh failed', error);
  }
}

if (sortSelect) {
  sortSelect.addEventListener('change', () => {
    renderBusList();
    renderMap();
  });
}

if (locateMeBtn) {
  locateMeBtn.addEventListener('click', () => {
    detectUserLocation();
    setTimeout(focusUserMarker, 800);
  });
}

function setMenuOpen(open) {
  if (!menuOverlay) {
    return;
  }

  menuOverlay.classList.toggle('open', open);
  document.body.classList.toggle('menu-open', open);
}

if (menuToggle && menuOverlay) {
  menuToggle.addEventListener('click', () => setMenuOpen(true));
}

if (menuClose && menuOverlay) {
  menuClose.addEventListener('click', () => setMenuOpen(false));
  menuOverlay.addEventListener('click', (event) => {
    if (event.target === menuOverlay) {
      setMenuOpen(false);
    }
  });
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    setMenuOpen(false);
  }
});

renderSummary(summarizeBuses(buses));
detectUserLocation();
renderBusList();
renderMap();
setInterval(refreshLiveData, 5000);
