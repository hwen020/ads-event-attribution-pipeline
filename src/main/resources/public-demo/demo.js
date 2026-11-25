(() => {
  "use strict";

  const searchCampaign = "10000000-0000-4000-8000-000000000001";
  const displayCampaign = "10000000-0000-4000-8000-000000000002";
  const socialCampaign = "10000000-0000-4000-8000-000000000003";
  const campaigns = new Map([
    [searchCampaign, "Search"],
    [displayCampaign, "Display"],
    [socialCampaign, "Social"]
  ]);
  const ids = new Map([
    ["impression", "20000000-0000-4000-8000-000000000001"],
    ["search", "20000000-0000-4000-8000-000000000002"],
    ["conversion", "20000000-0000-4000-8000-000000000003"],
    ["future", "20000000-0000-4000-8000-000000000004"],
    ["old-click", "20000000-0000-4000-8000-000000000005"],
    ["unattributed", "20000000-0000-4000-8000-000000000006"]
  ]);
  const stepLabels = {
    impression: "Impression",
    search: "Search click",
    conversion: "Purchase",
    duplicate: "Purchase retry",
    late: "Late Display click",
    future: "Post-conversion click",
    window: "Outside-window click",
    unattributed: "Unattributed purchase",
    conflict: "Conflicting event ID"
  };
  const stateKey = "ads-attribution-demo-session";
  const anchorKey = `${stateKey}-anchor`;
  const startButton = document.querySelector("#run-demo");
  const refreshButton = document.querySelector("#refresh");
  const resetButton = document.querySelector("#reset");
  const busyLabel = document.querySelector("#busy-state");
  const actionMessage = document.querySelector("#action-message");
  let sessionId = sessionStorage.getItem(stateKey);
  let snapshot = null;
  let running = false;
  let anchor = null;
  let sessionPromise = null;

  function setMessage(message, isError = false) {
    actionMessage.textContent = message;
    actionMessage.classList.toggle("error", isError);
  }

  function setBusy(busy, label) {
    running = busy;
    busyLabel.textContent = label;
    busyLabel.classList.toggle("busy", busy);
    startButton.disabled = busy;
    refreshButton.disabled = busy;
    resetButton.disabled = busy;
  }

  function formatTime(value) {
    if (!value) return "—";
    return new Intl.DateTimeFormat(undefined, {
      month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit"
    }).format(new Date(value));
  }

  function campaignName(id) {
    return id ? campaigns.get(id) || `Campaign ${id.slice(0, 8)}` : "No qualifying touch";
  }

  function resultClass(value) {
    return String(value || "").toLowerCase().replaceAll("_", "-");
  }

  function addText(parent, tag, value, className) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    node.textContent = value;
    parent.append(node);
    return node;
  }

  function render(data) {
    snapshot = data;
    document.querySelector("#session-expiry").textContent =
      `Space expires ${formatTime(data.expiresAt)} · ${data.eventCount}/100 events`;
    const metrics = data.metrics || {};
    document.querySelector("#metric-impressions").textContent = metrics.impressions || 0;
    document.querySelector("#metric-clicks").textContent = metrics.clicks || 0;
    document.querySelector("#metric-conversions").textContent = data.conversionCount || 0;
    document.querySelector("#metric-unattributed").textContent = metrics.unattributedConversions || 0;

    const eventsBody = document.querySelector("#events-body");
    eventsBody.replaceChildren();
    if (!data.events.length) {
      const row = eventsBody.insertRow();
      const cell = row.insertCell();
      cell.colSpan = 6;
      cell.className = "empty";
      cell.textContent = "Events sent by this walkthrough will appear here.";
    } else {
      data.events.forEach((event, index) => {
        const row = eventsBody.insertRow();
        const values = [
          String(index + 1).padStart(2, "0"),
          event.type === "CONVERSION" ? "Purchase" : `${event.type === "CLICK" ? "Click" : "Impression"} · ${campaignName(event.campaignId)}`,
          formatTime(event.eventTime),
          formatTime(event.receivedAt),
          formatTime(event.processedAt),
          event.outcome
        ];
        values.forEach((value, column) => {
          const cell = row.insertCell();
          if (column === 5) {
            const badge = document.createElement("span");
            badge.className = `result ${resultClass(value)}`;
            badge.textContent = value.replaceAll("_", " ");
            cell.append(badge);
          } else {
            cell.textContent = value;
          }
        });
      });
    }

    const attributionList = document.querySelector("#attributions");
    attributionList.replaceChildren();
    if (!data.attributions.length) {
      addText(attributionList, "p", "No conversions yet. Start the walkthrough to see credit assigned.", "empty");
    } else {
      data.attributions.forEach((attribution) => {
        const card = document.createElement("div");
        card.className = "attribution-card";
        const top = document.createElement("div");
        top.className = "attribution-top";
        addText(top, "strong", `${attribution.currency} ${Number(attribution.value).toFixed(2)}`);
        const badge = addText(top, "span", attribution.status, `result ${resultClass(attribution.status)}`);
        card.append(top);
        const detail = document.createElement("div");
        detail.className = "attribution-detail";
        const clickLine = attribution.touchId
          ? `Credited to ${campaignName(attribution.campaignId)} · click ${attribution.touchId.slice(0, 8)}…`
          : attribution.status === "UNATTRIBUTED"
            ? "No eligible impression or click received within its window."
            : "This conversion arrived outside the 48-hour eligibility limit.";
        detail.textContent = `${clickLine} · conversion at ${formatTime(attribution.eventTime)} · revision ${attribution.revision}`;
        card.append(detail);
        attributionList.append(card);
      });
    }

    const campaignBody = document.querySelector("#campaigns-body");
    campaignBody.replaceChildren();
    const campaignRows = Object.values(data.metrics?.campaigns || {});
    if (!campaignRows.length) {
      const row = campaignBody.insertRow();
      const cell = row.insertCell();
      cell.colSpan = 5;
      cell.className = "empty";
      cell.textContent = "Campaign results will appear here.";
    } else {
      campaignRows.sort((a, b) => campaignName(a.campaignId).localeCompare(campaignName(b.campaignId)));
      campaignRows.forEach((item) => {
        const row = campaignBody.insertRow();
        [
          campaignName(item.campaignId), item.impressions, item.clicks, item.conversions,
          Object.entries(item.revenue || {}).map(([currency, value]) => `${currency} ${Number(value).toFixed(2)}`).join(", ") || "—"
        ].forEach((value) => { row.insertCell().textContent = String(value); });
      });
    }
  }

  async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (sessionId) headers.set("X-Demo-Session", sessionId);
    if (options.body) headers.set("Content-Type", "application/json");
    const response = await fetch(path, {
      ...options,
      headers,
      cache: "no-store",
      signal: options.signal || AbortSignal.timeout(90_000)
    });
    if (!response.ok) {
      let detail = `Request failed (${response.status})`;
      try {
        const body = await response.json();
        if (body.detail) detail = body.detail;
      } catch (_) {
        if (response.status === 503) detail = "The free demo is waking up or at capacity. Please retry in a moment.";
      }
      if (response.status === 404 && sessionId) {
        sessionStorage.removeItem(stateKey);
        sessionStorage.removeItem(anchorKey);
        sessionId = null;
      }
      throw new Error(detail);
    }
    return response.status === 204 ? null : response.json();
  }

  async function ensureSession() {
    if (!sessionPromise) {
      sessionPromise = initializeSession();
    }
    try {
      await sessionPromise;
    } finally {
      sessionPromise = null;
    }
  }

  async function initializeSession() {
    if (sessionId) {
      try {
        render(await api("/api/v1/demo/state"));
        return;
      } catch (error) {
        if (sessionId) throw error;
      }
    }
    const created = await api("/api/v1/demo/sessions", { method: "POST" });
    sessionId = created.sessionId;
    sessionStorage.setItem(stateKey, sessionId);
    render(created);
  }

  async function stableEventId(label) {
    const bytes = new TextEncoder().encode(`${sessionId}:${label}`);
    const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)).slice(0, 16);
    hash[6] = (hash[6] & 0x0f) | 0x50;
    hash[8] = (hash[8] & 0x3f) | 0x80;
    const hex = [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  function timeAt(offsetMinutes) {
    return new Date(anchor.getTime() + offsetMinutes * 60_000).toISOString();
  }

  async function sendEvent(label, type, timeOffset, campaignId, conversion = false,
                           reuseId = null, receiveOffset = timeOffset + 1) {
    const eventId = reuseId || await stableEventId(label);
    const event = {
      eventId,
      userId: snapshot.userId,
      type,
      eventTime: timeAt(timeOffset),
      source: "public-browser-demo"
    };
    if (conversion) {
      event.conversionId = await stableEventId(`${label}-conversion`);
      event.conversionValue = "42.50";
      event.currency = "USD";
    } else {
      event.campaignId = campaignId;
      event.adId = campaignId;
    }
    const result = await api("/api/v1/demo/events", { method: "POST", body: JSON.stringify({
      event,
      simulatedReceivedAt: timeAt(receiveOffset)
    }) });
    render(result.state);
    return result;
  }

  async function runStep(name, action) {
    const node = document.querySelector(`[data-step="${name}"]`);
    node.classList.add("running");
    setMessage(`Processing: ${stepLabels[name]}…`);
    try {
      const result = await action();
      node.classList.remove("running");
      node.classList.add("done");
      const labels = {
        ACCEPTED: "accepted",
        DUPLICATE: "duplicate safely ignored",
        CONFLICT: "conflict detected; original payload retained",
        LATE: "outside lateness allowance",
        DUPLICATE_CONVERSION: "duplicate conversion ID ignored"
      };
      setMessage(`${stepLabels[name]}: ${labels[result.outcome] || result.outcome}.`);
    } catch (error) {
      node.classList.remove("running");
      node.classList.add("failed");
      setMessage(`${stepLabels[name]} failed: ${error.message}. Retry is safe with the same event ID.`, true);
      throw error;
    }
  }

  function clearProgress() {
    document.querySelectorAll(".step-list li").forEach((step) => step.classList.remove("running", "done", "failed"));
  }

  async function runWalkthrough() {
    if (running) return;
    setBusy(true, "Sending events…");
    clearProgress();
    try {
      await ensureSession();
      anchor = new Date(sessionStorage.getItem(anchorKey) || Date.now() - 2 * 60_000);
      sessionStorage.setItem(anchorKey, anchor.toISOString());
      const impressionId = await stableEventId("impression");
      await runStep("impression", () => sendEvent("impression", "IMPRESSION", -25, displayCampaign));
      await runStep("search", () => sendEvent("search", "CLICK", -15, searchCampaign));
      await runStep("conversion", () => sendEvent("conversion", "CONVERSION", -10, null, true));
      await runStep("duplicate", async () => {
        const original = snapshot.events.find((event) => event.type === "CONVERSION");
        const event = {
          eventId: original.eventId, userId: snapshot.userId, type: "CONVERSION",
          eventTime: original.eventTime, source: "public-browser-demo",
          conversionId: snapshot.attributions[0].conversionId,
          conversionValue: "42.50", currency: "USD"
        };
        const result = await api("/api/v1/demo/events", { method: "POST", body: JSON.stringify({
          event,
          simulatedReceivedAt: snapshot.events.find((item) => item.eventId === original.eventId).receivedAt
        }) });
        render(result.state);
        return result;
      });
      await runStep("late", () => sendEvent("late-display-click", "CLICK", -12,
        displayCampaign, false, null, -8));
      await runStep("future", () => sendEvent("future-click", "CLICK", 2, socialCampaign));
      await runStep("window", () => sendEvent("old-click", "CLICK", -10 * 24 * 60,
        searchCampaign, false, null, -10 * 24 * 60 + 1));
      await runStep("unattributed", () => sendEvent("unattributed-purchase", "CONVERSION",
        -47 * 60, null, true, null, -47 * 60 + 1));
      await runStep("conflict", () => sendEvent("conflicting-impression", "IMPRESSION", -25, socialCampaign, false, impressionId));
      setMessage("Walkthrough complete. Search credit moved to Display after the late click; the duplicate did not add a conversion.");
      busyLabel.textContent = "Complete";
    } catch (_) {
      busyLabel.textContent = "Needs attention";
    } finally {
      setBusy(false, busyLabel.textContent);
    }
  }

  async function refresh() {
    if (running) return;
    setBusy(true, "Refreshing…");
    try {
      await ensureSession();
      setMessage("Results refreshed from your backend demo space.");
      busyLabel.textContent = "Ready";
    } catch (error) {
      setMessage(error.message, true);
      busyLabel.textContent = "Unavailable";
    } finally {
      setBusy(false, busyLabel.textContent);
    }
  }

  async function reset() {
    if (running || !window.confirm("Reset only this browser tab's synthetic demo data?")) return;
    setBusy(true, "Resetting…");
    clearProgress();
    try {
      await ensureSession();
      render(await api("/api/v1/demo/reset", { method: "POST" }));
      sessionStorage.removeItem(anchorKey);
      anchor = null;
      setMessage("Your demo space is empty. Other browser sessions are not affected.");
      busyLabel.textContent = "Ready";
    } catch (error) {
      setMessage(error.message, true);
      busyLabel.textContent = "Unavailable";
    } finally {
      setBusy(false, busyLabel.textContent);
    }
  }

  startButton.addEventListener("click", runWalkthrough);
  refreshButton.addEventListener("click", refresh);
  resetButton.addEventListener("click", reset);

  ensureSession().then(() => setMessage("Your isolated workspace is ready. Start the walkthrough when you are ready."))
    .catch((error) => setMessage(error.message, true));
})();
