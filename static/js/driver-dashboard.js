(function () {
    const activeTripData = document.getElementById('driverActiveTripData');
    const activeTrip = activeTripData ? JSON.parse(activeTripData.textContent || 'null') : null;
    const driverEndpoints = document.body.dataset;
    const startTripForm = document.getElementById('startTripForm');
    const endTripBtn = document.getElementById('endTripBtn');
    const trackingBtn = document.getElementById('trackingBtn');
    const trackingStatus = document.getElementById('trackingStatus');
    const gpsState = document.getElementById('gpsState');
    let watchId = null;

    if (startTripForm) {
      startTripForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const formData = new FormData(startTripForm);
        const response = await fetch(driverEndpoints.startTripUrl, {
          method: 'POST',
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
        const response = await fetch(driverEndpoints.endTripUrl, { method: 'POST' });
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
          'Content-Type': 'application/json'
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
        async (position) => {
          const latitude = position.coords.latitude;
          const longitude = position.coords.longitude;

          try {
            const result = await pushLocation(latitude, longitude);
            if (result.success) {
              setTrackingStatus(`GPS active. Last update: ${new Date().toLocaleTimeString()}`);
            } else {
              setTrackingStatus(result.error || 'Location update failed.', true);
            }
          } catch (error) {
            setTrackingStatus('GPS update failed. Check server connection.', true);
          }
        },
        () => {
          setTrackingStatus('Location permission denied or unavailable.', true);
          if (trackingBtn) {
            trackingBtn.textContent = 'GPS Permission Required';
            trackingBtn.disabled = true;
          }
        },
        {
          enableHighAccuracy: true,
          maximumAge: 5000,
          timeout: 10000
        }
      );
    }

    if (!activeTrip && gpsState) {
      gpsState.textContent = 'Waiting for trip start';
    }

    if (activeTrip) {
      sessionStorage.removeItem('codexmbs_auto_track');
      startTracking();
    }
})();
