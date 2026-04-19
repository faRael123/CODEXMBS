(function () {
    const activeTripData = document.getElementById('driverActiveTripData');
    const activeTrip = activeTripData ? JSON.parse(activeTripData.textContent || 'null') : null;
    const driverEndpoints = document.body.dataset;
    const csrfHeaders = driverEndpoints.csrfToken ? { 'X-CSRFToken': driverEndpoints.csrfToken } : {};
    const startTripForm = document.getElementById('startTripForm');
    const endTripBtn = document.getElementById('endTripBtn');
    const trackingBtn = document.getElementById('trackingBtn');
    const trackingStatus = document.getElementById('trackingStatus');
    const gpsState = document.getElementById('gpsState');
    const LOCATION_REFRESH_MS = 3000;
    const MIN_LOCATION_SEND_MS = 2500;
    const GEO_OPTIONS = {
      enableHighAccuracy: true,
      maximumAge: 1000,
      timeout: 5000
    };
    let watchId = null;
    let locationPollId = null;
    let locationPushInFlight = false;
    let lastLocationSentAt = 0;

    if (startTripForm) {
      startTripForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const formData = new FormData(startTripForm);
        const response = await fetch(driverEndpoints.startTripUrl, {
          method: 'POST',
          headers: csrfHeaders,
          body: formData
        });
        const result = await response.json();
        if (result.success) {
          sessionStorage.setItem('codexmbs_auto_track', '1');
          window.location.reload();
          return;
        }
        alert(result.error || 'Could not start trip.');
      });
    }

    if (endTripBtn) {
      endTripBtn.addEventListener('click', async () => {
        if (!window.confirm('Are you sure you want to end this trip?')) {
          return;
        }
        if (watchId !== null && navigator.geolocation) {
          navigator.geolocation.clearWatch(watchId);
          watchId = null;
        }
        if (locationPollId !== null) {
          clearInterval(locationPollId);
          locationPollId = null;
        }
        const response = await fetch(driverEndpoints.endTripUrl, { method: 'POST', headers: csrfHeaders });
        const result = await response.json();
        if (result.success) {
          window.location.reload();
          return;
        }
        alert(result.error || 'Could not end trip.');
      });
    }

    if (activeTrip && activeTrip.started_at) {
      const startTime = new Date(activeTrip.started_at.replace(' ', 'T'));
      const durationNode = document.getElementById('tripDuration');
      const updateDuration = () => {
        const elapsed = Math.max(0, Date.now() - startTime.getTime());
        const hours = String(Math.floor(elapsed / 3600000)).padStart(2, '0');
        const minutes = String(Math.floor((elapsed % 3600000) / 60000)).padStart(2, '0');
        const seconds = String(Math.floor((elapsed % 60000) / 1000)).padStart(2, '0');
        if (durationNode) {
          durationNode.textContent = `${hours}:${minutes}:${seconds}`;
        }
      };
      updateDuration();
      setInterval(updateDuration, 1000);
    }

    async function pushLocation(latitude, longitude) {
      const response = await fetch(driverEndpoints.driverLocationUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...csrfHeaders
        },
        body: JSON.stringify({ latitude, longitude })
      });
      return response.json();
    }

    function setTrackingStatus(message, isError = false) {
      if (!trackingStatus) return;
      trackingStatus.textContent = message;
      trackingStatus.classList.toggle('error', isError);
      trackingStatus.classList.toggle('live', !isError && message.toLowerCase().includes('active'));
      if (gpsState) {
        gpsState.textContent = isError ? 'GPS needs attention' : 'GPS running';
      }
    }

    async function handlePosition(position) {
      const now = Date.now();
      if (locationPushInFlight || now - lastLocationSentAt < MIN_LOCATION_SEND_MS) {
        return;
      }

      locationPushInFlight = true;
      try {
        const result = await pushLocation(position.coords.latitude, position.coords.longitude);
        if (result.success) {
          lastLocationSentAt = Date.now();
          setTrackingStatus(`GPS active. Last update: ${new Date().toLocaleTimeString()}`);
        } else {
          setTrackingStatus(result.error || 'Location update failed.', true);
        }
      } catch (error) {
        setTrackingStatus('GPS update failed. Check server connection.', true);
      } finally {
        locationPushInFlight = false;
      }
    }

    function handleLocationError() {
      setTrackingStatus('Location permission denied or unavailable.', true);
      if (trackingBtn) {
        trackingBtn.textContent = 'GPS Permission Required';
        trackingBtn.disabled = true;
      }
    }

    function requestCurrentPosition() {
      navigator.geolocation.getCurrentPosition(handlePosition, handleLocationError, GEO_OPTIONS);
    }

    function startTracking() {
      if (!activeTrip || !navigator.geolocation) {
        setTrackingStatus('Geolocation is not supported on this device.', true);
        return;
      }

      if (watchId !== null) {
        setTrackingStatus('GPS transmission is already running.');
        return;
      }

      setTrackingStatus('Waiting for current location...');
      if (trackingBtn) {
        trackingBtn.textContent = 'GPS Locked On';
        trackingBtn.disabled = true;
      }

      watchId = navigator.geolocation.watchPosition(
        handlePosition,
        handleLocationError,
        GEO_OPTIONS
      );
      requestCurrentPosition();
      locationPollId = setInterval(requestCurrentPosition, LOCATION_REFRESH_MS);
    }

    if (!activeTrip && gpsState) {
      gpsState.textContent = 'Waiting for trip start';
    }

    if (activeTrip) {
      sessionStorage.removeItem('codexmbs_auto_track');
      startTracking();
    }
})();
