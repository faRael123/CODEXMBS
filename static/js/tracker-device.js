(function () {
    const activeTripData = document.getElementById('trackerActiveTripData');
    const activeTrip = activeTripData ? JSON.parse(activeTripData.textContent || 'null') : null;
    const trackerEndpoints = document.body.dataset;
    const startTripForm = document.getElementById('startTripForm');
    const endTripBtn = document.getElementById('endTripBtn');
    const trackingBtn = document.getElementById('trackingBtn');
    const trackingStatus = document.getElementById('trackingStatus');
    const gpsStateLabel = document.getElementById('gpsStateLabel');
    const gpsDetail = document.getElementById('gpsDetail');
    const tripStateLabel = document.getElementById('tripStateLabel');
    const tripStateDetail = document.getElementById('tripStateDetail');
    const latitudeValue = document.getElementById('latitudeValue');
    const longitudeValue = document.getElementById('longitudeValue');
    const accuracyValue = document.getElementById('accuracyValue');
    const speedValue = document.getElementById('speedValue');
    const lastSentValue = document.getElementById('lastSentValue');
    const wakeLockStatus = document.getElementById('wakeLockStatus');
    const wakeLockValue = document.getElementById('wakeLockValue');
    let watchId = null;
    let wakeLock = null;

    function setTrackingStatus(message, isError = false) {
      if (trackingStatus) {
        trackingStatus.textContent = message;
        trackingStatus.classList.toggle('error', isError);
        trackingStatus.classList.toggle('live', !isError && message.toLowerCase().includes('gps active'));
      }
      if (gpsStateLabel) {
        gpsStateLabel.textContent = isError ? 'GPS needs attention' : 'GPS running';
      }
      if (gpsDetail) {
        gpsDetail.textContent = message;
      }
    }

    function updateTelemetry(position) {
      const coords = position.coords;
      if (latitudeValue) latitudeValue.textContent = coords.latitude.toFixed(6);
      if (longitudeValue) longitudeValue.textContent = coords.longitude.toFixed(6);
      if (accuracyValue) accuracyValue.textContent = `${Math.round(coords.accuracy)} m`;
      if (speedValue) {
        speedValue.textContent = coords.speed === null ? '0 km/h' : `${Math.max(coords.speed * 3.6, 0).toFixed(1)} km/h`;
      }
    }

    async function requestWakeLock() {
      if (!('wakeLock' in navigator)) {
        if (wakeLockStatus) wakeLockStatus.textContent = 'Wake lock unavailable';
        if (wakeLockValue) wakeLockValue.textContent = 'Unsupported';
        return;
      }

      try {
        wakeLock = await navigator.wakeLock.request('screen');
        if (wakeLockStatus) wakeLockStatus.textContent = 'Screen wake lock active';
        if (wakeLockValue) wakeLockValue.textContent = 'Active';
        wakeLock.addEventListener('release', () => {
          if (wakeLockStatus) wakeLockStatus.textContent = 'Wake lock released';
          if (wakeLockValue) wakeLockValue.textContent = 'Released';
        });
      } catch (error) {
        if (wakeLockStatus) wakeLockStatus.textContent = 'Wake lock denied';
        if (wakeLockValue) wakeLockValue.textContent = 'Denied';
      }
    }

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible' && activeTrip) {
        requestWakeLock();
      }
    });

    async function pushLocation(position) {
      const coords = position.coords;
      const response = await fetch(trackerEndpoints.driverLocationUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          latitude: coords.latitude,
          longitude: coords.longitude,
          accuracy: coords.accuracy,
          speed: coords.speed,
          heading: coords.heading
        })
      });
      return response.json();
    }

    function handleLocationError() {
      setTrackingStatus('Location permission denied or GPS unavailable.', true);
      if (trackingBtn) {
        trackingBtn.textContent = 'Enable GPS';
        trackingBtn.disabled = false;
      }
    }

    function startTracking() {
      if (!activeTrip || !navigator.geolocation) {
        setTrackingStatus('Geolocation is not supported on this device.', true);
        return;
      }

      if (watchId !== null) {
        setTrackingStatus('GPS active. Waiting for the next location refresh.');
        return;
      }

      setTrackingStatus('Waiting for current location...');
      if (trackingBtn) {
        trackingBtn.textContent = 'GPS Enabled';
        trackingBtn.disabled = true;
      }

      requestWakeLock();

      watchId = navigator.geolocation.watchPosition(
        async (position) => {
          updateTelemetry(position);

          try {
            const result = await pushLocation(position);
            if (result.success) {
              const sentAt = new Date().toLocaleTimeString();
              if (lastSentValue) lastSentValue.textContent = sentAt;
              setTrackingStatus(`GPS active. Last update sent at ${sentAt}.`);
            } else {
              setTrackingStatus(result.error || 'Location update failed.', true);
            }
          } catch (error) {
            setTrackingStatus('GPS update failed. Check server connection.', true);
          }
        },
        handleLocationError,
        {
          enableHighAccuracy: true,
          maximumAge: 5000,
          timeout: 10000
        }
      );
    }

    if (startTripForm) {
      startTripForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const formData = new FormData(startTripForm);
        const response = await fetch(trackerEndpoints.startTripUrl, {
          method: 'POST',
          body: formData
        });
        const result = await response.json();
        if (result.success) {
          sessionStorage.setItem('codexmbs_auto_track_terminal', '1');
          window.location.reload();
          return;
        }
        alert(result.error || 'Could not start trip.');
      });
    }

    if (endTripBtn) {
      endTripBtn.addEventListener('click', async () => {
        if (watchId !== null && navigator.geolocation) {
          navigator.geolocation.clearWatch(watchId);
          watchId = null;
        }
        if (wakeLock) {
          await wakeLock.release().catch(() => {});
        }
        const response = await fetch(trackerEndpoints.endTripUrl, { method: 'POST' });
        const result = await response.json();
        if (result.success) {
          window.location.reload();
          return;
        }
        alert(result.error || 'Could not end trip.');
      });
    }

    if (activeTrip) {
      if (tripStateLabel) tripStateLabel.textContent = 'Trip Active';
      if (tripStateDetail) tripStateDetail.textContent = `${activeTrip.plate_number} on ${activeTrip.route_name}`;
    }

    if (trackingBtn) {
      trackingBtn.addEventListener('click', startTracking);
    }

    if (activeTrip && sessionStorage.getItem('codexmbs_auto_track_terminal') === '1') {
      sessionStorage.removeItem('codexmbs_auto_track_terminal');
      startTracking();
    } else if (activeTrip) {
      requestWakeLock();
    }
})();
