(function () {
    const ticketPrintContextNode = document.getElementById('conductorTicketPrintContext');
    const conductorLiveEndpoint = document.body.dataset.conductorLiveEndpoint || '';
    const conductorLocationEndpoint = document.body.dataset.conductorLocationEndpoint || '';
    const csrfHeaders = document.body.dataset.csrfToken ? { 'X-CSRFToken': document.body.dataset.csrfToken } : {};
    const ticketPrintContext = ticketPrintContextNode ? JSON.parse(ticketPrintContextNode.textContent || '{}') : {};
    const trackingStatus = document.getElementById('trackingStatus');
    const currentStop = document.getElementById('currentStop');
    const currentCoords = document.getElementById('currentCoords');
    const lastUpdate = document.getElementById('lastUpdate');
    const summaryOrigin = document.getElementById('summaryOrigin');
    const originStopInput = document.getElementById('originStopInput');
    const destinationStopInput = document.getElementById('destinationStopInput');
    const passengerTypeInput = document.getElementById('passengerTypeInput');
    const summaryDestination = document.getElementById('summaryDestination');
    const summaryPassengerType = document.getElementById('summaryPassengerType');
    const summaryFare = document.getElementById('summaryFare');
    const ticketMockForm = document.getElementById('ticketMockForm');
    const saveMockButton = document.getElementById('saveMockButton');
    const previewMockButton = document.getElementById('previewMockButton');
    const currentOccupancy = document.getElementById('currentOccupancy');
    const destinationButtons = Array.from(document.querySelectorAll('.destination-chip'));
    const passengerButtons = Array.from(document.querySelectorAll('.passenger-chip'));
    const LOCATION_REFRESH_MS = 3000;
    const LIVE_STATUS_REFRESH_MS = 3000;
    const MIN_LOCATION_SEND_MS = 2500;
    const GEO_OPTIONS = {
      enableHighAccuracy: true,
      maximumAge: 1000,
      timeout: 5000
    };
    let selectedDestinationButton = null;
    let selectedPassengerButton = null;
    let conductorWatchId = null;
    let conductorPollId = null;
    let conductorLocationInFlight = false;
    let lastConductorLocationSentAt = 0;

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      }[char]));
    }

    function currentFareValue() {
      const passengerType = passengerTypeInput ? passengerTypeInput.value : '';
      if (!selectedDestinationButton || !passengerType) {
        return 0;
      }
      return Number(selectedDestinationButton.dataset[`fare${passengerType.charAt(0).toUpperCase()}${passengerType.slice(1)}`] || 0);
    }

    function updateTicketSummary() {
      const passengerType = passengerTypeInput ? passengerTypeInput.value : '';
      const destination = destinationStopInput ? destinationStopInput.value : '';
      const fareValue = currentFareValue();
      const isReady = Boolean(destination && passengerType);

      if (summaryDestination) summaryDestination.textContent = destination || 'Select destination';
      if (summaryPassengerType) summaryPassengerType.textContent = passengerType ? passengerType.charAt(0).toUpperCase() + passengerType.slice(1) : 'Select passenger type';
      if (summaryFare) summaryFare.textContent = `PHP ${Math.round(fareValue || 0)}`;
      if (saveMockButton) saveMockButton.disabled = !isReady;
      if (previewMockButton) previewMockButton.disabled = !isReady;
    }

    function openTicketPrintMock(options = {}) {
      const shouldAutoPrint = options.autoPrint !== false;
      const passengerType = passengerTypeInput ? passengerTypeInput.value : '';
      const destination = destinationStopInput ? destinationStopInput.value : '';
      const origin = summaryOrigin ? summaryOrigin.textContent.trim() : '';
      const fareValue = Math.round(currentFareValue() || 0);

      if (!passengerType || !destination) {
        return;
      }

      const transactionId = Date.now().toString().slice(-5);
      const printedAt = new Date();
      const dateLabel = printedAt.toLocaleDateString('en-PH', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      });
      const timeLabel = printedAt.toLocaleTimeString('en-PH', {
        hour: '2-digit',
        minute: '2-digit'
      });
      const routeLabel = `${ticketPrintContext.routeStart} - ${ticketPrintContext.routeEnd}`;
      const ticketWindow = window.open('', 'gajoda_ticket_print_mock', 'width=420,height=760');

      if (!ticketWindow) {
        return false;
      }

      ticketWindow.document.write(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Ticket Print Mock</title>
          <style>
            @page { size: 58mm auto; margin: 0; }
            * { box-sizing: border-box; }
            body {
              margin: 0;
              background: #f2f2f2;
              color: #111;
              font-family: "Courier New", Consolas, monospace;
            }
            .print-toolbar {
              display: flex;
              flex-wrap: wrap;
              gap: 8px;
              justify-content: center;
              align-items: center;
              padding: 12px;
              font-family: Arial, sans-serif;
            }
            .print-toolbar span {
              width: 100%;
              color: #555;
              font-size: 12px;
              text-align: center;
            }
            .print-toolbar button {
              border: 0;
              border-radius: 8px;
              padding: 9px 12px;
              background: #d60000;
              color: #fff;
              font-weight: 700;
              cursor: pointer;
            }
            .ticket-paper {
              width: 58mm;
              min-height: 92mm;
              margin: 0 auto 18px;
              padding: 5mm 4mm 8mm;
              background: #fff;
              box-shadow: 0 18px 38px rgba(0, 0, 0, 0.18);
              overflow: hidden;
            }
            .receipt-rule {
              margin: 2mm 0;
              border: 0;
              border-top: 1px dashed #111;
            }
            .ticket-title {
              text-align: center;
              font-weight: 700;
              letter-spacing: 0;
              margin-bottom: 1mm;
              font-size: 12px;
              text-transform: uppercase;
            }
            .ticket-subtitle {
              text-align: center;
              font-size: 8px;
              letter-spacing: 0;
              margin: 0;
            }
            .ticket-row {
              display: grid;
              grid-template-columns: 20mm 1fr;
              gap: 2mm;
              font-size: 9px;
              line-height: 1.25;
              margin: 1.2mm 0;
              word-break: break-word;
            }
            .ticket-label {
              color: #111;
              font-weight: 700;
            }
            .route-line {
              margin: 2mm 0 1mm;
              font-size: 9px;
              line-height: 1.15;
              text-align: left;
              word-break: break-word;
            }
            .route-line strong { display: block; }
            .fare-total {
              display: flex;
              justify-content: space-between;
              align-items: flex-end;
              gap: 3mm;
              margin-top: 3mm;
              font-size: 10px;
              font-weight: 700;
            }
            .fare-total strong {
              display: inline-block;
              font-size: 20px;
              line-height: 1;
              letter-spacing: 0;
            }
            .mock-note {
              margin: 5mm 0 0;
              text-align: center;
              font-size: 6px;
              color: #111;
            }
            .paper-feed {
              height: 8mm;
            }
            @media print {
              body { background: #fff; }
              .print-toolbar { display: none; }
              .ticket-paper {
                width: 58mm;
                min-height: 0;
                margin: 0;
                box-shadow: none;
              }
            }
          </style>
        </head>
        <body>
          <div class="print-toolbar">
            <span>${shouldAutoPrint ? '58mm Bluetooth thermal layout. Select the paired printer in the print dialog.' : 'Receipt preview only. Use Print when you want to test the browser print dialog.'}</span>
            <button onclick="window.print()">Print</button>
            <button onclick="window.close()">Close</button>
          </div>
          <main class="ticket-paper">
            <div class="ticket-title">${escapeHtml(ticketPrintContext.operator)}</div>
            <p class="ticket-subtitle">58MM THERMAL RECEIPT</p>
            <hr class="receipt-rule">
            <div class="ticket-row"><span class="ticket-label">Plate No:</span><span>${escapeHtml(ticketPrintContext.plateNumber)}</span></div>
            <div class="ticket-row"><span class="ticket-label">Bus #:</span><span>${escapeHtml(ticketPrintContext.busNumber.replace(/\D+/g, '') || ticketPrintContext.busNumber)}</span></div>
            <div class="ticket-row"><span class="ticket-label">Ticket ID:</span><span>${escapeHtml(transactionId)}</span></div>
            <div class="ticket-row"><span class="ticket-label">Date:</span><span>${escapeHtml(dateLabel)}</span></div>
            <div class="ticket-row"><span class="ticket-label">Time:</span><span>${escapeHtml(timeLabel)}</span></div>
            <hr class="receipt-rule">
            <div class="route-line">
              <strong>Route: ${escapeHtml(routeLabel)}</strong>
            </div>
            <div class="ticket-row"><span class="ticket-label">Origin:</span><span>${escapeHtml(origin)}</span></div>
            <div class="ticket-row"><span class="ticket-label">Destination:</span><span>${escapeHtml(destination)}</span></div>
            <div class="ticket-row"><span class="ticket-label">Fare Type:</span><span>${escapeHtml(passengerType.charAt(0).toUpperCase() + passengerType.slice(1))}</span></div>
            <hr class="receipt-rule">
            <div class="fare-total"><span>TOTAL PHP</span><strong>${escapeHtml(fareValue.toFixed(2))}</strong></div>
            <p class="mock-note">MOCK RECEIPT - FOR PRINT LAYOUT TESTING ONLY</p>
            <div class="paper-feed"></div>
          </main>
          <script>
            window.focus();
            ${shouldAutoPrint ? "window.addEventListener('load', () => setTimeout(() => window.print(), 500));" : ''}
          <\/script>
        </body>
        </html>
      `);
      ticketWindow.document.close();
      return true;
    }

    destinationButtons.forEach((button) => {
      button.addEventListener('click', () => {
        destinationButtons.forEach((item) => item.classList.remove('is-active'));
        button.classList.add('is-active');
        selectedDestinationButton = button;
        if (destinationStopInput) destinationStopInput.value = button.dataset.destination || '';
        updateTicketSummary();
      });
    });

    passengerButtons.forEach((button) => {
      button.addEventListener('click', () => {
        passengerButtons.forEach((item) => item.classList.remove('is-active'));
        button.classList.add('is-active');
        selectedPassengerButton = button;
        if (passengerTypeInput) passengerTypeInput.value = button.dataset.passengerType || '';
        updateTicketSummary();
      });
    });

    if (ticketMockForm) {
      ticketMockForm.addEventListener('submit', () => {
        openTicketPrintMock();
      });
    }

    if (previewMockButton) {
      previewMockButton.addEventListener('click', () => {
        openTicketPrintMock({ autoPrint: false });
      });
    }

    async function refreshTripLocation() {
      const response = await fetch(conductorLiveEndpoint, { cache: 'no-store' });
      if (!response.ok) {
        throw new Error('Failed to load trip location');
      }
      const payload = await response.json();
      if (!payload.active) {
        if (trackingStatus) trackingStatus.textContent = 'No active trip';
        return;
      }
      if (trackingStatus) trackingStatus.textContent = payload.tracking ? 'Live GPS active' : 'Waiting for GPS';
      const stopLabel = payload.stop_name || 'On route';
      if (currentStop) currentStop.textContent = stopLabel;
      if (currentCoords) currentCoords.textContent = stopLabel;
      if (summaryOrigin) summaryOrigin.textContent = stopLabel;
      if (originStopInput) originStopInput.value = stopLabel;
      if (lastUpdate) lastUpdate.textContent = payload.recorded_at || 'No live update yet';
      if (currentOccupancy && payload.occupancy !== undefined && payload.capacity !== undefined) {
        currentOccupancy.textContent = `${payload.occupancy}/${payload.capacity}`;
      }
    }

    async function pushConductorLocation(latitude, longitude) {
      if (!conductorLocationEndpoint) {
        return { success: false };
      }
      const response = await fetch(conductorLocationEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...csrfHeaders
        },
        body: JSON.stringify({ latitude, longitude })
      });
      return response.json();
    }

    function startConductorGpsFallback() {
      if (!navigator.geolocation || !conductorLocationEndpoint || conductorWatchId !== null) {
        return;
      }

      const handleConductorPosition = async (position) => {
        const now = Date.now();
        if (conductorLocationInFlight || now - lastConductorLocationSentAt < MIN_LOCATION_SEND_MS) {
          return;
        }

        conductorLocationInFlight = true;
        try {
          const result = await pushConductorLocation(position.coords.latitude, position.coords.longitude);
          if (result.success && trackingStatus) {
            lastConductorLocationSentAt = Date.now();
            trackingStatus.textContent = 'Live GPS active';
          }
        } catch (error) {
          if (trackingStatus) trackingStatus.textContent = 'Conductor GPS standby';
        } finally {
          conductorLocationInFlight = false;
        }
      };

      const handleConductorLocationError = () => {
        if (trackingStatus) trackingStatus.textContent = 'Waiting for GPS permission';
      };

      const requestConductorPosition = () => {
        navigator.geolocation.getCurrentPosition(handleConductorPosition, handleConductorLocationError, GEO_OPTIONS);
      };

      conductorWatchId = navigator.geolocation.watchPosition(handleConductorPosition, handleConductorLocationError, GEO_OPTIONS);
      requestConductorPosition();
      conductorPollId = setInterval(requestConductorPosition, LOCATION_REFRESH_MS);
    }

    updateTicketSummary();
    startConductorGpsFallback();
    refreshTripLocation().catch(() => {
      if (trackingStatus) trackingStatus.textContent = 'Waiting for GPS';
    });
    setInterval(() => refreshTripLocation().catch(() => {
      if (trackingStatus) trackingStatus.textContent = 'GPS refresh failed';
    }), LIVE_STATUS_REFRESH_MS);
})();
