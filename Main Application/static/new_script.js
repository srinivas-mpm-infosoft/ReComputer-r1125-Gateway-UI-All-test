// Global config object
let config = {};
//
//function loadConfig() {
//  fetch('config.json')
//    .then(response => response.json())
//    .then(data => {
//      config = data;
//      console.log("Config loaded", config);
//
//      // Now it’s safe to render, my dear
//      renderComPortPanel();
//      renderIOSettingsPanel();
//    })
//    .catch(err => console.error("Error loading config", err));
//}
async function loadConfig() {
  const res = await fetch("/config");
  const cfg = await res.json();
  config = cfg;
  renderIOSettingsPanel();
}

async function saveConfig() {
  console.log("Config: ", config);

  const res = await fetch("/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (res.ok) alert("Configuration Saved!");
}

function showTab(tabId) {
  document.querySelectorAll(".tab").forEach((t) => (t.style.display = "none"));
  document.getElementById(tabId).style.display = "block";
}
console.log(config);

if (!config.ioSettings) config.ioSettings = {};
config.ioSettings.analog = {
  pollingInterval: 30,
  pollingIntervalUnit: "Sec",
  saveLog: true,
  channels: [
    { enabled: true, resolution: "16 bit", mode: "0-10V" },
    { enabled: true, resolution: "16 bit", mode: "0-10V" },
    { enabled: true, resolution: "16 bit", mode: "0-10V" },
    { enabled: true, resolution: "16 bit", mode: "4-20mA" },
  ],
  extensionADC: [
    { resolution: "16 bit", mode: "4-20mA" },
    { resolution: "16 bit", mode: "4-20mA" },
    { resolution: "16 bit", mode: "4-20mA" },
  ],
  scaling: [
    { min: 20, max: 40 },
    { min: 25, max: 35 },
    { min: 30, max: 45 },
    { min: 2, max: 8 },
    { min: 4, max: 10 },
    { min: 0, max: 10 },
    { min: 5, max: 8 },
    { min: 10, max: 30 },
  ],
};

if (!config.wireless)
  config.wireless = {
    communicationMedia: "4G/LTE",
    sendFormat: "FTP", // FTP, JSON, MQTT, Disable
    ftp: {
      serverIP: "",
      username: "",
      password: "",
      port: 21,
      logFolder: "",
    },
    pollingTimeUnit: "Sec",
    pollingInterval: 5,

    apn: "airtelgprs.com",
  };

// === PANEL RENDER FUNCTIONS ===

function updateAnalogRange(ch, savedRange = null) {
  const modeSelect = document.getElementById(`ch_mode_${ch}`);
  const rangeSelect = document.getElementById(`ch_range_${ch}`);

  if (!modeSelect || !rangeSelect) return;

  const mode = modeSelect.value;

  // Clear existing options
  rangeSelect.innerHTML = "";

  let validRanges = [];

  if (mode === "0-10V") {
    validRanges = ["0-5V", "0-10V"];
  } else if (mode === "4-20mA") {
    validRanges = ["4-20mA", "0-20mA"]; // remove 0-20mA if HW doesn't support it
  }

  // Populate dropdown
  validRanges.forEach((r) => {
    const opt = document.createElement("option");
    opt.value = r;
    opt.textContent = r;
    rangeSelect.appendChild(opt);
  });

  // Restore saved range if valid, else default to first option
  if (savedRange && validRanges.includes(savedRange)) {
    rangeSelect.value = savedRange;
  } else {
    rangeSelect.selectedIndex = 0;
  }
}

function renderIOSettingsPanel(subTab = "Settings") {
  let html = `
    <div class="panel-header">I/O Settings</div>
    <div class="tab-list">
      <button class="tab-btn ${
        subTab == "Settings" ? "active" : ""
      }" data-tab="Settings">Settings</button>
      <button class="tab-btn ${
        subTab == "Analog" ? "active" : ""
      }" data-tab="Analog">Analog</button>
      <button class="tab-btn ${
        subTab == "Digital Input" ? "active" : ""
      }" data-tab="Digital Input">Digital I/O</button>
    </div>
    <div class="tab-content">`;

  /* ================= SETTINGS ================= */
  if (subTab === "Settings") {
    html += `
      <form id="io-settings-form">
        <label><input type="checkbox" name="modbus" ${
          config.ioSettings.settings.modbus ? "checked" : ""
        }> Modbus RTU</label>
        <label><input type="checkbox" name="analog" ${
          config.ioSettings.settings.analog ? "checked" : ""
        }> Analog</label>
        <label><input type="checkbox" name="digitalInput" ${
          config.ioSettings.settings.digitalInput ? "checked" : ""
        }> Digital Input</label>
        <button type="submit" class="button-primary">Read</button>
        <span class="checkmark" id="save-tick" style="display:none">&#10004;</span>
        <button class="button-primary" type="button" onclick="downloadConfig()">Export Config</button>
      </form>
    `;
  } else if (subTab === "Analog") {
    const a = config.ioSettings.analog;
    html += `
        <form id="analog-form">
          <label>
            <input type="radio" name="intervalUnit" value="Sec" ${
              a.pollingIntervalUnit === "Sec" ? "checked" : ""
            }> Sec
          </label>
          <label>
            <input type="radio" name="intervalUnit" value="Min" ${
              a.pollingIntervalUnit === "Min" ? "checked" : ""
            }> Min
          </label>
          <label>
            <input type="radio" name="intervalUnit" value="Hour" ${
              a.pollingIntervalUnit === "Hour" ? "checked" : ""
            }> Hour
          </label>
          &nbsp;
          Analog Polling Interval:
          <input type="number" name="pollingInterval" style="width:60px" min="1" max="9999" value="${
            a.pollingInterval
          }"> (sec)
          <br><br>
          <section class="analog-input">
            <b>Analog Input</b>
            <table class="channel-table">
              <tr><th>Enable</th><th>Mode</th><th>Scaling</th></tr>
<tr>
  <td>
    <input type="checkbox" name="ch_enable_0" ${
      a.channels[0].enabled ? " checked" : ""
    }>
  </td>

  <td>
    <select name="ch_mode_0"
            id="ch_mode_0"
            onchange="updateAnalogRange(0)">
      <option value="0-10V" ${
        a.channels[0].mode === "0-10V" ? "selected" : ""
      }>0-10V</option>
      <option value="4-20mA" ${
        a.channels[0].mode === "4-20mA" ? "selected" : ""
      }>4-20mA</option>
    </select>
  </td>

  <td>
    <select name="ch_range_0" id="ch_range_0"></select>
  </td>
</tr>

            </table>
          </section>
          <br>
          <button class="button-primary" type="submit">Save</button>
          <span class="checkmark" id="analog-save-tick" style="display:none">&#10004;</span>
        </form>
      `;
  } else if (subTab === "Digital Input") {
  /* ================= DIGITAL I/O ================= */
    const di = config.ioSettings.digitalInput || { channels: [false, false] };
    const doo = config.ioSettings.digitalOutput || { channels: [0, 0] };

    html += `
      <form id="digital-io-form">

        <h3>Digital Input</h3>
        <table class="channel-table">
          <tr><th>Channel</th><th>Enable</th></tr>
          ${[0, 1]
            .map(
              (i) => `
            <tr>
              <td>DI ${i + 1}</td>
              <td><input type="checkbox" name="di_channel_${i}" ${
                di.channels[i] ? "checked" : ""
              }></td>
            </tr>
          `
            )
            .join("")}
        </table>

        <br>

        <h3>Digital Output</h3>
        <table class="channel-table">
          <tr><th>Channel</th><th>State</th></tr>
          ${[0, 1]
            .map(
              (i) => `
            <tr>
              <td>DO ${i + 1}</td>
              <td>
                <select name="do_channel_${i}">
                  <option value="0" ${
                    doo.channels[i] === 0 ? "selected" : ""
                  }>LOW</option>
                  <option value="1" ${
                    doo.channels[i] === 1 ? "selected" : ""
                  }>HIGH</option>
                </select>
              </td>
            </tr>
          `
            )
            .join("")}
        </table>

        <br>
        <button type="submit" class="button-primary">Apply</button>
        <span class="checkmark" id="dio-tick" style="display:none">&#10004;</span>
        <button class="button-primary" type="button" onclick="downloadConfig()">Export Config</button>
      </form>
    `;
  }

  html += "</div>";
  document.getElementById("main-panel").innerHTML = html;

  /* ================= TAB HANDLERS ================= */
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.onclick = () => renderIOSettingsPanel(btn.dataset.tab);
  });

  html += "</div>";
  document.getElementById("main-panel").innerHTML = html;

  if (subTab === "Analog") {
    const a = config.ioSettings.analog;

    requestAnimationFrame(() => {
      updateAnalogRange(0, a.channels[0].range);
    });
  }

  // Setup tab buttons
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.onclick = () => renderIOSettingsPanel(btn.getAttribute("data-tab"));
  });

/* ================= SETTINGS SUBMIT ================= */
  if (subTab === "Settings") {
    const f = document.getElementById("io-settings-form");
    f.onsubmit = e => {
      e.preventDefault();
      config.ioSettings.settings.modbus = f.modbus.checked;
      config.ioSettings.settings.analog = f.analog.checked;
      config.ioSettings.settings.digitalInput = f.digitalInput.checked;
      saveConfig();
      showTick("save-tick");
    };
  }

  /* ================= DIGITAL I/O SUBMIT (FIXED) ================= */
  if (subTab === "Digital Input") {
    const f = document.getElementById("digital-io-form");
    if (!f) return;

    f.onsubmit = e => {
      e.preventDefault();

      const fd = new FormData(f);

      config.ioSettings.digitalInput = {
        channels: [0,1].map(i => fd.get(`di_channel_${i}`) === "on")
      };

      config.ioSettings.digitalOutput = {
        channels: [0,1].map(i => parseInt(fd.get(`do_channel_${i}`), 10))
      };

      saveConfig();
      showTick("dio-tick");
    };
  }

  /* ================= ANALOG SUBMIT ================= */
  if (subTab === "Analog") {
    const f = document.getElementById("analog-form");
    f.onsubmit = e => {
      e.preventDefault();
      const a = config.ioSettings.analog;
      a.pollingInterval = parseInt(f.pollingInterval.value, 10);
      a.pollingIntervalUnit = f.intervalUnit.value;
      a.channels[0].enabled = f["ch_enable_0"].checked;
      a.channels[0].mode = f["ch_mode_0"].value;
      a.channels[0].range = f["ch_range_0"].value;
      saveConfig();
      showTick("analog-save-tick");
    };
  }

}

function initDigitalInputScript(initialMode) {
  function updateTableColumns(mode) {
    const countHeaders = document.querySelectorAll(".count-col-header");
    const countCells = document.querySelectorAll(".count-col");
    const timeHeaders = document.querySelectorAll(".time-col-header");
    const timeCells = document.querySelectorAll(".time-col");

    if (mode === "digital") {
      countHeaders.forEach((h) => (h.style.display = "none"));
      countCells.forEach((c) => (c.style.display = "none"));
      timeHeaders.forEach((h) => (h.style.display = "none"));
      timeCells.forEach((c) => (c.style.display = "none"));
    } else if (mode === "counter") {
      countHeaders.forEach((h) => (h.style.display = "table-cell"));
      countCells.forEach((c) => (c.style.display = "table-cell"));
      timeHeaders.forEach((h) => (h.style.display = "none"));
      timeCells.forEach((c) => (c.style.display = "none"));
    } else if (mode === "time") {
      countHeaders.forEach((h) => (h.style.display = "none"));
      countCells.forEach((c) => (c.style.display = "none"));
      timeHeaders.forEach((h) => (h.style.display = "table-cell"));
      timeCells.forEach((c) => (c.style.display = "table-cell"));
    }
  }

  // Init with saved mode
  updateTableColumns(initialMode);

  // Listen for radio button changes
  document.querySelectorAll('input[name="mode"]').forEach((radio) => {
    radio.addEventListener("change", () => {
      updateTableColumns(radio.value);
    });
  });
}

function renderNetworkPanel() {
  const APN_MAP = {
    jio: "jionet",
    airtel: "airtelgprs.com",
    bsnl: "bsnlnet",
    vi: "www",
    other: "",
  };

  // ---------- HARD GUARANTEES ----------
  if (!config.network) config.network = {};

  if (!config.network.wifi) {
    config.network.wifi = { ssid: "", password: "" };
  }

  if (!config.network.sim4g) {
    config.network.sim4g = { provider: "", apn: "" };
  }

  if (!config.network.static) {
    config.network.static = {
      enabled: false,
      ip: "",
      subnet: "",
      gateway: "",
      dns_primary: "",
      dns_secondary: "",
    };
  }

  const net = config.network;

  // ---------- UI ----------
  document.getElementById("main-panel").innerHTML = `
      <div class="panel-header">Network Configuration</div>

      <div class="tab-list">
        <button class="active" data-tab="wifi">Wi-Fi</button>
        <button data-tab="sim">4G SIM</button>
        <button data-tab="static">Static IP</button>
      </div>

      <!-- Wi-Fi -->
      <div class="tab-content" id="wifi-tab">
        <fieldset class="net-fieldset">
          <legend>Wi-Fi Credentials</legend>

          <label>SSID</label>
          <input type="text" id="wifi-ssid" value="${net.wifi.ssid}">

          <label>Password</label>
          <input type="password" id="wifi-password" value="${
            net.wifi.password
          }">
        </fieldset>

        <button class="button-primary" id="save-wifi">Save Wi-Fi</button>
        <span class="checkmark" id="wifi-tick" style="display:none">✔</span>
      </div>

      <!-- SIM -->
      <div class="tab-content" id="sim-tab" style="display:none">
        <fieldset class="net-fieldset">
          <legend>4G SIM (APN)</legend>

          <label>Provider</label>
          <select id="apn-provider">
            <option value="">Select provider</option>
            ${Object.keys(APN_MAP)
              .map(
                (p) => `
                  <option value="${p}" ${
                  net.sim4g.provider === p ? "selected" : ""
                }>${p.toUpperCase()}</option>`
              )
              .join("")}
          </select>

          <label>APN</label>
          <input type="text" id="apn-value" value="${net.sim4g.apn}">
        </fieldset>

        <button class="button-primary" id="save-sim">Save SIM</button>
        <span class="checkmark" id="sim-tick" style="display:none">✔</span>
      </div>

      <!-- Static IP -->
      <div class="tab-content" id="static-tab" style="display:none">
        <fieldset class="net-fieldset">
          <legend>Static IP Configuration</legend>

          <label>
            <input type="checkbox" id="static-enable" ${
              net.static.enabled ? "checked" : ""
            }>
            Enable Static IP
          </label>

          <label>IP Address</label>
          <input type="text" id="static-ip" value="${net.static.ip}">

          <label>Subnet Mask</label>
          <input type="text" id="static-subnet" value="${net.static.subnet}">

          <label>Gateway</label>
          <input type="text" id="static-gw" value="${net.static.gateway}">

          <label>Preferred DNS Server</label>
          <input type="text" id="static-dns1" value="${net.static.dns_primary}">

          <label>Alternate DNS Server</label>
          <input type="text" id="static-dns2" value="${
            net.static.dns_secondary
          }">
        </fieldset>

        <button class="button-primary" id="save-static">Save Static IP</button>
        <span class="checkmark" id="static-tick" style="display:none">✔</span>
      </div>
    `;

  // ---------- Tabs ----------
  document.querySelectorAll(".tab-list button").forEach((btn) => {
    btn.onclick = () => {
      document
        .querySelectorAll(".tab-list button")
        .forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      ["wifi", "sim", "static"].forEach((t) => {
        document.getElementById(`${t}-tab`).style.display =
          btn.dataset.tab === t ? "block" : "none";
      });
    };
  });

  // ---------- APN logic ----------
  const providerSel = document.getElementById("apn-provider");
  const apnInput = document.getElementById("apn-value");

  providerSel.onchange = () => {
    if (!providerSel.value) {
      apnInput.value = "";
      apnInput.disabled = true;
      return;
    }

    if (providerSel.value === "other") {
      apnInput.disabled = false;
      apnInput.value = "";
    } else {
      apnInput.value = APN_MAP[providerSel.value];
      apnInput.disabled = true;
    }
  };
  providerSel.onchange();

  // ---------- Static enable toggle ----------
  const toggleStaticFields = () => {
    const en = document.getElementById("static-enable").checked;
    [
      "static-ip",
      "static-subnet",
      "static-gw",
      "static-dns1",
      "static-dns2",
    ].forEach((id) => {
      document.getElementById(id).disabled = !en;
    });
  };

  document.getElementById("static-enable").onchange = toggleStaticFields;
  toggleStaticFields();

  // ---------- Save handlers ----------
  document.getElementById("save-wifi").onclick = async () => {
    net.wifi.ssid = document.getElementById("wifi-ssid").value.trim();
    net.wifi.password = document.getElementById("wifi-password").value;
    await saveConfig();
    showTick("wifi-tick");
  };

  document.getElementById("save-sim").onclick = async () => {
    net.sim4g.provider = providerSel.value;
    net.sim4g.apn = apnInput.value.trim();
    await saveConfig();
    showTick("sim-tick");
  };

  document.getElementById("save-static").onclick = async () => {
    net.static.enabled = document.getElementById("static-enable").checked;
    net.static.ip = document.getElementById("static-ip").value.trim();
    net.static.subnet = document.getElementById("static-subnet").value.trim();
    net.static.gateway = document.getElementById("static-gw").value.trim();
    net.static.dns_primary = document
      .getElementById("static-dns1")
      .value.trim();
    net.static.dns_secondary = document
      .getElementById("static-dns2")
      .value.trim();

    await saveConfig();
    showTick("static-tick");
  };
}

function renderChangePasswordPanel() {
  document.getElementById("main-panel").innerHTML = `
        <div class="panel-header">Change Password</div>
        <div class="tab-content">
          <form id="change-password-form">
            <label>Old Password:</label>
            <input type="password" name="oldPassword" required>
            <br><br>
            <label>New Password:</label>
            <input type="password" name="newPassword" required>
            <br><br>
            <label>Confirm New Password:</label>
            <input type="password" name="confirmPassword" required>
            <br><br>
            <button class="button-primary" type="submit">Update Password</button>
            <span id="password-tick" style="display:none;color:#49ba3c;font-size:18px;">✔ Updated</span>
          </form>
        </div>
      `;

  let form = document.getElementById("change-password-form");
  form.onsubmit = async function (e) {
    e.preventDefault();

    let oldPassword = form.oldPassword.value;
    let newPassword = form.newPassword.value;
    let confirmPassword = form.confirmPassword.value;
    const login_req = false;
    if (newPassword !== confirmPassword) {
      alert("New passwords do not match!");
      return;
    }

    let res = await fetch("/reset-password", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ oldPassword, newPassword, login_req }),
    });

    if (res.ok) {
      document.getElementById("password-tick").style.display = "inline";
      form.reset();
    } else {
      let data = await res.json();
      alert(data.error || "Password change failed!");
    }
  };
}

// Inject CSS via JS
(function injectStyles() {
  const css = `
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; color:#222; margin:16px; }
      .panel-header { font-size:18px; font-weight:600; margin-bottom:8px; }
      .tab-list { display:flex; flex-wrap:wrap; gap:10px; }
      .tab-btn, .brand-tab, .slave-tab {
        padding:6px 10px; border:1px solid #cfd4dc; background:#f7f9fc; cursor:pointer; border-radius:10px;
        transition: all .15s ease;
      }
      .tab-btn[aria-selected="true"], .brand-tab[aria-selected="true"] { background:#0b5; color:#fff; border-color:#0b5; font-weight:600; }
      .slave-tab[aria-selected="true"] { background:#005bbb; color:#fff; border-color:#005bbb; }
      .tab-content { border:1px solid #e5e7eb; padding:12px; background:#fff; border-radius:8px; }
      .button-primary { background:#0b5; color:#fff; padding:6px 12px; border:none; border-radius:8px; cursor:pointer; }
      .button { background:#eef2f7; color:#111; padding:6px 10px; border:1px solid #cfd4dc; border-radius:8px; cursor:pointer; }
      .chip-del { color:#c00; background:transparent; border:none; cursor:pointer; margin-left:6px; font-size:16px; }
      .toolbar { display:flex; gap:8px; align-items:center; }
      .split-toolbar { display:flex; justify-content:space-between; align-items:center; gap:8px; }
      .channel-table { border-collapse:collapse; min-width:900px; width:100%; }
      .channel-table th, .channel-table td { border:1px solid #d9dee5; padding:6px; }
      .modal { position:fixed; inset:0; background:rgba(0,0,0,0.35); display:none; align-items:center; justify-content:center; }
      .modal .modal-content { background:#fff; min-width:360px; border-radius:8px; padding:12px; }
      label { margin-right:12px; }
      .sr-only { position:absolute; left:-10000px; width:1px; height:1px; overflow:hidden; }
      .card { border:1px solid #d9dee5; border-radius:8px; padding:12px; background:#fff; margin-bottom:12px; }
      .accordion-h { display:flex; justify-content:space-between; align-items:center; padding:8px; background:#f3f6fb; border-radius:6px; cursor:pointer; }
      .soft { color:#666; }
      `;
  const style = document.createElement("style");
  style.type = "text/css";
  style.innerText = css;
  document.head.appendChild(style);
})();

function ensureBase() {
  if (!config.ModbusRTU) config.ModbusRTU = {};
  if (!config.ModbusRTU.energyMeter)
    config.ModbusRTU.energyMeter = { brands: {}, order: [], globalPresets: {} };
  if (!config.ModbusRTU.energyMeter.globalPresets)
    config.ModbusRTU.energyMeter.globalPresets = {};
  if (!config.ModbusRTU.settings)
    config.ModbusRTU.settings = {
      baudRate: "9600",
      parity: "Even",
      dataBits: 8,
      stopBits: 1,
    };
  if (!config.plc_configurations) config.plc_configurations = [];
  if (!config.ModbusRTU.transmitters) config.ModbusRTU.transmitters = [];
}

// Optional role; safe fallback to guest
const uiState = { role: "guest" };
async function fetchRole() {
  try {
    const r = await fetch("/whoami", { credentials: "include" });
    if (!r.ok) throw new Error("whoami failed");
    const data = await r.json();
    uiState.role = (data.role || "guest").toLowerCase();
  } catch (e) {
    uiState.role = "guest";
  }
}

// Built-in schema for EM6436H
const builtInSchemas = {
  schneider_em6436h: {
    label: "Schneider EM6436H",
    rows: [
      //Currents (updated start addresses)
      {
        name: "Current L1",
        start: 3000,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
      {
        name: "Current L2",
        start: 3002,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
      {
        name: "Current L3",
        start: 3004,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },

      // L-L Voltages
      {
        name: "Voltage L1-L2",
        start: 3020,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L2-L3",
        start: 3022,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L3-L1",
        start: 3024,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },

      // L-N Voltages
      {
        name: "Voltage L1-N",
        start: 3028,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L2-N",
        start: 3030,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L3-N",
        start: 3032,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },

      // Power Factor
      {
        name: "Power Factor Total",
        start: 3084,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian / Little Word Order",
        length: 2,
        enabled: true,
      },
    ],
  },
}; // Confirm addresses with vendor docs before production

// Utilities
function esc(s) {
  return (s ?? "").toString().replace(
    /[&<>\"']/g,
    (m) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      }[m])
  );
}
function numOrEmpty(v, def = "") {
  return v == null || Number.isNaN(v) ? def : v;
}
function deepClone(x) {
  return JSON.parse(JSON.stringify(x));
}
function slugify(s) {
  return (
    s
      .toLowerCase()
      .trim()
      .replace(/\s+/g, "_")
      .replace(/[^\w\-]+/g, "")
      .slice(0, 60) || "preset_" + Date.now()
  );
}
function suggestNextSlaveId(slaves) {
  const ids = (slaves || []).map((s) => Number(s.id)).filter(Number.isInteger);
  let n = 1;
  while (ids.includes(n)) n++;
  return n;
}
function flashTick(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = "inline";
  setTimeout(() => (el.style.display = "none"), 1200);
}

// Router (updated to keep tabs for PLC/Transmitter)
async function renderModBusRTUPanel(
  activeTopTab = "Settings",
  brandKey = null,
  activeSlaveId = null
) {
  ensureBase();
  if (!renderModBusRTUPanel._roleFetched) {
    await fetchRole();
    renderModBusRTUPanel._roleFetched = true;
  }

  const tabs = [
    { name: "Settings", label: "Settings" },
    { name: "EnergyMeter", label: "External Module" },
    // {name:"PLC", label:"PLC"},
    // {name:"Transmitter", label:"Transmitter"}
  ];
  const topTabsHtml = `
        <div class="tab-list" role="tablist" aria-label="Main sections">
          ${tabs
            .map(
              (t) => `
            <button role="tab" class="tab-btn" aria-selected="${
              t.name === activeTopTab ? "true" : "false"
            }"
                    tabindex="${
                      t.name === activeTopTab ? "0" : "-1"
                    }" data-top="${t.name}">
              ${t.label}
            </button>`
            )
            .join("")}
          <button id="tabs-actions-trigger" class="button" style="margin-left:8px;">＋ Tabs</button>
        </div>
      `;

  if (activeTopTab === "Settings") {
    renderSettings(topTabsHtml);
    wireTopTabs(activeTopTab, brandKey, activeSlaveId);
    wireTabsActions();
    return;
  }
  if (activeTopTab === "EnergyMeter") {
    renderEnergyMeter(topTabsHtml, brandKey, activeSlaveId);
    wireTopTabs(activeTopTab, brandKey, activeSlaveId);
    wireTabsActions();
    return;
  }
  if (activeTopTab === "PLC") {
    const inner = rtuPlcInnerHtml();
    const panel = document.getElementById("main-panel");
    panel.innerHTML = `
          ${topTabsHtml}
          <div class="tab-content" role="tabpanel" style="margin-top:8px;">
            ${inner}
          </div>
        `;
    wireTopTabs(activeTopTab, brandKey, activeSlaveId);
    wireTabsActions();
    bindRtuPlcInnerEvents();
    return;
  }
  if (activeTopTab === "Transmitter") {
    const inner = transmitterInnerHtml();
    const panel = document.getElementById("main-panel");
    panel.innerHTML = `
          <div class="panel-header">Transmitters</div>
          ${topTabsHtml}
          <div class="tab-content" role="tabpanel" style="margin-top:8px;">
            ${inner}
          </div>
        `;
    wireTopTabs(activeTopTab, brandKey, activeSlaveId);
    wireTabsActions();
    bindTransmitterEvents();
    return;
  }

  const panel = document.getElementById("main-panel");
  panel.innerHTML = `
        <div class="panel-header">${activeTopTab}</div>
        ${topTabsHtml}
        <div class="tab-content" role="tabpanel" style="margin-top:8px;">Coming soon…</div>
      `;
  wireTopTabs(activeTopTab, brandKey, activeSlaveId);
  wireTabsActions();
}

function wireTopTabs(activeTopTab, brandKey, activeSlaveId) {
  document.querySelectorAll('[role="tab"][data-top]').forEach((btn) => {
    btn.onclick = () =>
      renderModBusRTUPanel(
        btn.getAttribute("data-top"),
        brandKey,
        activeSlaveId
      );
  });
}

function wireTabsActions() {
  const btn = document.getElementById("tabs-actions-trigger");
  if (!btn) return;
  btn.onclick = () => {
    const action = prompt("Type: add <Name> or remove <Name>");
    if (!action) return;
    const [cmd, ...rest] = action.split(" ");
    const name = rest.join(" ").trim();
    if (!name) return;
    if (cmd.toLowerCase() === "add") {
      const panel = document.getElementById("main-panel");
      panel.innerHTML = `
            <div class="panel-header">${esc(name)}</div>
            <div class="tab-content" role="tabpanel" style="margin-top:8px;">
              <div class="soft">Custom tab "${esc(name)}".</div>
              <div style="margin-top:12px;"><button class="button-primary" id="custom-save">Save</button></div>
            </div>
          `;
      document.getElementById("custom-save").onclick = () => {
        saveConfig();
        alert("Saved.");
      };
    } else if (cmd.toLowerCase() === "remove") {
      alert(
        "Custom tab removal is ephemeral in this demo. Reload resets custom tabs."
      );
    }
  };
}

// Settings
function renderSettings(topTabsHtml) {
  const s = config.ModbusRTU.settings;
  const html = `
        <div class="panel-header">Settings</div>
        ${topTabsHtml}
        <div class="tab-content" role="tabpanel" style="margin-top:8px;">
          <form id="rtu-settings-form">
            <label>Baud Rate:
              <select name="baudRate">
                ${[
                  "300",
                  "600",
                  "1200",
                  "2400",
                  "4800",
                  "9600",
                  "19200",
                  "38400",
                  "57600",
                  "115200",
                  "230400",
                ]
                  .map(
                    (b) => `
                  <option value="${b}" ${
                      String(s.baudRate) === b ? "selected" : ""
                    }>${b}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Parity:
              <select name="parity">
                ${["Even", "Odd", "None"]
                  .map(
                    (p) => `
                  <option value="${p}" ${
                      s.parity === p ? "selected" : ""
                    }>${p}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Data Bits:
              <select name="dataBits">
                ${[5, 6, 7, 8]
                  .map(
                    (d) =>
                      `<option value="${d}" ${
                        Number(s.dataBits) === d ? "selected" : ""
                      }>${d}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Stop Bits:
              <select name="stopBits" id="stopBits">
                ${[1, 2]
                  .map(
                    (v) =>
                      `<option value="${v}" ${
                        Number(s.stopBits) === v ? "selected" : ""
                      }>${v}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <div style="margin-top:8px;color:#666;font-size:12px;">
              Even/Odd parity commonly uses 1 stop bit; None often pairs with 2 stop bits for Modbus RTU reliability.
            </div>
            <br>
            <div class="toolbar">
              <button type="submit" class="button-primary">Save</button>
              <span class="checkmark" id="rtu-settings-tick" style="display:none;">&#10004;</span>
            </div>
          </form>
        </div>
      `;
  document.getElementById("main-panel").innerHTML = html;

  const form = document.getElementById("rtu-settings-form");
  const paritySel = form.parity,
    stopSel = form.stopBits;
  paritySel.onchange = () => {
    stopSel.value = paritySel.value === "None" ? "2" : "1";
  };
  form.onsubmit = (e) => {
    e.preventDefault();
    config.ModbusRTU.settings = {
      baudRate: form.baudRate.value,
      parity: form.parity.value,
      dataBits: Number(form.dataBits.value),
      stopBits: Number(form.stopBits.value),
    };
    saveConfig();
    flashTick("rtu-settings-tick");
  };
}

// Energy Meter (brands as tabs; slaves as tabs; presets; manual save)
function renderEnergyMeter(
  topTabsHtml,
  currentBrandKey = null,
  currentSlaveId = null
) {
  const em = config.ModbusRTU.energyMeter;
  if (em.order.length === 0) {
    const k = "waveshare_4-20ma_analog_acquisition_module";
    if (!em.brands[k])
      em.brands[k] = {
        label:
          builtInSchemas[k]?.label ||
          "Waveshare 4-20 mA Analog Acquisition Module",
        slaves: [],
        registersBySlave: {},
        presets: {},
      };
    em.order.push(k);
  }
  const brandKey = currentBrandKey || em.order[0];
  const brand = em.brands[brandKey];

  const brandTabs = `
        <div class="tab-list" role="tablist" aria-label="Brands" style="margin-top:12px;">
          ${em.order
            .map((k) => {
              const lbl = em.brands[k]?.label || k;
              const sel = k === brandKey;
              return `
              <span style="display:inline-flex;align-items:center;">
                <button role="tab" class="brand-tab" aria-selected="${
                  sel ? "true" : "false"
                }" tabindex="${
                sel ? "0" : "-1"
              }" data-brand="${k}">${lbl}</button>
                <button class="chip-del em-brand-del" data-brand="${k}" title="Remove Brand">×</button>
              </span>
            `;
            })
            .join("")}
          <button id="em-add-brand" class="button-primary" style="margin-left:6px;">＋ Brand</button>
        </div>
      `;

  const slaves = brand.slaves || [];
  const slaveId = currentSlaveId || (slaves[0]?.id ?? null);
  const slaveTabs = `
        <div style="margin-top:12px;">
          <div class="tab-list" role="tablist" aria-label="Slaves">
            ${slaves
              .map((s) => {
                const sel = String(s.id) === String(slaveId);
                return `
                <span class="slave-pill" data-slave="${
                  s.id
                }" style="display:inline-flex;align-items:center;">
                  <button role="tab" class="slave-tab" aria-selected="${
                    sel ? "true" : "false"
                  }" tabindex="${sel ? "0" : "-1"}" data-slave="${s.id}">
                    <span class="slave-label" data-slave="${s.id}">${
                  s.id
                }</span>
                    <input class="slave-input" type="number" min="1" max="247" data-slave="${
                      s.id
                    }" value="${
                  s.id
                }" style="display:none;width:64px;margin-left:4px;">
                  </button>
                  <button class="chip-del em-slave-del" data-slave="${
                    s.id
                  }" title="Remove Slave">×</button>
                </span>
              `;
              })
              .join("")}
            <button id="em-add-slave" class="button" style="margin-left:8px;">＋ Slave</button>
          </div>
        </div>
      `;

  const regKey = String(slaveId);
  const rows =
    slaveId == null ? [] : deepClone(brand.registersBySlave?.[regKey] || []);
  const builtIn = builtInSchemas[brandKey]
    ? [
        {
          id: `builtin:${brandKey}`,
          name: `Built-in: ${builtInSchemas[brandKey].label}`,
        },
      ]
    : [];
  const brandPresets = Object.entries(brand.presets || {}).map(([pid, p]) => ({
    id: `brand:${pid}`,
    name: `Brand: ${p.name || pid}`,
  }));
  const globalPresets = Object.entries(em.globalPresets || {}).map(
    ([pid, p]) => ({ id: `global:${pid}`, name: `Global: ${p.name || pid}` })
  );
  const presetToolbar = `
        <select id="em-preset-select">
          <option value="">Load preset…</option>
          ${builtIn
            .map((p) => `<option value="${p.id}">${p.name}</option>`)
            .join("")}
          ${
            brandPresets.length
              ? `<optgroup label="Brand presets">${brandPresets
                  .map((p) => `<option value="${p.id}">${p.name}</option>`)
                  .join("")}</optgroup>`
              : ""
          }
          ${
            globalPresets.length
              ? `<optgroup label="Global presets">${globalPresets
                  .map((p) => `<option value="${p.id}">${p.name}</option>`)
                  .join("")}</optgroup>`
              : ""
          }
        </select>
        <button id="em-load-preset-btn" class="button">Load</button>
        <select id="em-preset-scope" class="button"><option value="">Select to save the preset for Brand/Global</option><option value="brand">Brand</option><option value="global">Global</option></select>
        <button id="em-save-preset-btn" class="button">Save as Preset</button>
      `;

  const regsTable =
    slaveId == null
      ? `<div style="margin-top:8px;color:#666;">Select a slave to view registers.</div>`
      : `
        <div style="margin-top:10px;">
          <div class="toolbar" style="justify-content:space-between;">
            <b>Registers for Slave ${slaveId}</b>
            <div class="toolbar">
              ${presetToolbar}
              <button id="em-add-row" class="button">＋ Row</button>
            </div>
          </div>
          <table class="channel-table" id="em-regs-table" style="margin-top:8px;">
            <tr>
              <th>S.No</th>
              <th>Name</th>
              <th>Start Address</th>
              <th>Offset</th>
              <th>Type</th>
              <th>Conversion</th>
              <th>Length</th>
              <th>Process Range</th>
              <th>Enabled</th>
              <th>Remove</th>
            </tr>
            ${rows.map((r, i) => rowHtml(r, i)).join("")}
          </table>

        </div>
      `;

  const html = `
        <div class="panel-header">External Module</div>
        ${topTabsHtml}
        <div class="tab-content" role="tabpanel" style="margin-top:8px;">
          ${brandTabs}
          ${slaveTabs}
          ${regsTable}
          <div style="margin-top:12px;" class="toolbar">
            <button id="em-save" class="button-primary">Save</button>
            <span class="checkmark" id="em-tick" style="display:none;">&#10004;</span>
          </div>
        </div>
        ${brandAddModal()}
        ${presetNameModal()}
      `;
  document.getElementById("main-panel").innerHTML = html;

  // Brand handlers
  document.querySelectorAll('.brand-tab[role="tab"]').forEach((b) => {
    b.onclick = () =>
      renderEnergyMeter(topTabsHtml, b.getAttribute("data-brand"), null);
  });
  document.querySelectorAll(".em-brand-del").forEach((b) => {
    b.onclick = () => {
      const k = b.getAttribute("data-brand");
      if (
        !confirm(
          `Remove brand "${
            em.brands[k]?.label || k
          }"? This deletes its slaves and registers.`
        )
      )
        return;
      if (em.brands[k]) delete em.brands[k];
      em.order = em.order.filter((x) => x !== k);
      saveConfig();
      const next = em.order[0] || null;
      renderModBusRTUPanel("EnergyMeter", next, null);
    };
  });
  document.getElementById("em-add-brand")?.addEventListener("click", () => {
    openBrandModal((label) => {
      const key =
        label.toLowerCase().replace(/\W+/g, "_") || "brand_" + Date.now();
      if (!em.brands[key]) {
        em.brands[key] = {
          label,
          slaves: [],
          registersBySlave: {},
          presets: {},
        };
        em.order.push(key);
      } else {
        em.brands[key].label = label;
        if (!em.order.includes(key)) em.order.push(key);
      }
      saveConfig();
      renderModBusRTUPanel("EnergyMeter", key, null);
    });
  });

  // Slave handlers
  document.querySelectorAll('.slave-tab[role="tab"]').forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const id = btn.getAttribute("data-slave");
      const selected = btn.getAttribute("aria-selected") === "true";
      const label = btn.querySelector(".slave-label");
      const input = btn.querySelector(".slave-input");
      if (!selected) {
        renderEnergyMeter(topTabsHtml, brandKey, id);
        return;
      }
      // Inline edit
      label.style.display = "none";
      input.style.display = "inline-block";
      input.focus();
      input.select();
      const cancel = () => {
        input.style.display = "none";
        label.style.display = "inline";
        input.value = label.textContent.trim();
        cleanup();
      };
      const commit = () => {
        const newVal = input.value.trim();
        const n = parseInt(newVal, 10);
        if (!Number.isInteger(n) || n < 1 || n > 247) {
          alert("Slave ID must be 1..247");
          cancel();
          return;
        }
        const oldId = String(id);
        if (String(n) === oldId) {
          cancel();
          return;
        }
        if ((brand.slaves || []).some((s) => String(s.id) === String(n))) {
          alert("Slave ID already exists.");
          cancel();
          return;
        }
        brand.slaves = (brand.slaves || []).map((s) =>
          String(s.id) === oldId ? { id: n } : s
        );
        brand.registersBySlave = brand.registersBySlave || {};
        brand.registersBySlave[String(n)] = brand.registersBySlave[oldId] || [];
        delete brand.registersBySlave[oldId];
        renderEnergyMeter(topTabsHtml, brandKey, String(n));
      };
      const onKey = (ev) => {
        if (ev.key === "Enter") commit();
        else if (ev.key === "Escape") cancel();
      };
      const onBlur = () => commit();
      function cleanup() {
        input.removeEventListener("keydown", onKey);
        input.removeEventListener("blur", onBlur);
      }
      input.addEventListener("keydown", onKey);
      input.addEventListener("blur", onBlur);
    });
  });
  document.querySelectorAll(".em-slave-del").forEach((b) => {
    b.onclick = () => {
      const sid = String(b.getAttribute("data-slave"));
      if (!confirm(`Remove slave ${sid}?`)) return;
      brand.slaves = (brand.slaves || []).filter((s) => String(s.id) !== sid);
      if (brand.registersBySlave) delete brand.registersBySlave[sid];
      saveConfig();
      const next = brand.slaves[0]?.id ?? null;
      renderEnergyMeter(topTabsHtml, brandKey, next);
    };
  });
  document.getElementById("em-add-slave")?.addEventListener("click", () => {
    const nextId = suggestNextSlaveId(brand.slaves || []);
    brand.slaves = brand.slaves || [];
    brand.slaves.push({ id: nextId });
    brand.registersBySlave = brand.registersBySlave || {};
    brand.registersBySlave[String(nextId)] =
      brand.registersBySlave[String(nextId)] || [];
    saveConfig();
    renderEnergyMeter(topTabsHtml, brandKey, String(nextId));
  });

  // Registers and presets
  if (slaveId != null) {
    let workingRows = rows;
    const table = document.getElementById("em-regs-table");
    table?.addEventListener("input", (e) => {
      const idx = Number(e.target.getAttribute("data-index"));
      const field = e.target.getAttribute("data-field");
      if (!Number.isInteger(idx) || !field) return;
      let v = e.target.type === "checkbox" ? e.target.checked : e.target.value;
      if (
        ["start", "offset", "length"].includes(field) &&
        e.target.type !== "checkbox"
      )
        v = parseInt(v, 10);
      workingRows[idx] = workingRows[idx] || {};
      workingRows[idx][field] = v;
    });
    table?.addEventListener("click", (e) => {
      const btn = e.target.closest(".em-reg-remove");
      if (!btn) return;
      const idx = Number(btn.getAttribute("data-index"));
      if (idx >= 0 && idx < workingRows.length) workingRows.splice(idx, 1);
      rerenderRegsTable(workingRows);
    });
    document.getElementById("em-add-row")?.addEventListener("click", () => {
      workingRows.push({
        name: "",
        start: "",
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        process_min: "",
        process_max: "",
        enabled: false,
      });

      rerenderRegsTable(workingRows);
    });
    const emLoad = document.getElementById("em-load-preset-btn");
    const emSel = document.getElementById("em-preset-select");
    emLoad?.addEventListener("click", () => {
      const sel = emSel.value;
      if (!sel) return;
      if (sel.startsWith("builtin:")) {
        const key = sel.split(":")[1];
        workingRows = deepClone(builtInSchemas[key]?.rows || []);
      } else if (sel.startsWith("brand:")) {
        const pid = sel.split(":")[1];
        workingRows = deepClone((brand.presets || {})[pid]?.rows || []);
      } else if (sel.startsWith("global:")) {
        const pid = sel.split(":")[1];
        workingRows = deepClone((em.globalPresets || {})[pid]?.rows || []);
      }
      rerenderRegsTable(workingRows);
    });
    const savePresetBtn = document.getElementById("em-save-preset-btn");
    const scopeSel = document.getElementById("em-preset-scope");
    savePresetBtn?.addEventListener("click", () => {
      openPresetNameModal((presetName) => {
        if (!presetName) return;
        const pid = slugify(presetName);
        if (scopeSel.value === "global") {
          em.globalPresets[pid] = {
            name: presetName,
            rows: deepClone(workingRows),
          };
        } else {
          brand.presets = brand.presets || {};
          brand.presets[pid] = {
            name: presetName,
            rows: deepClone(workingRows),
          };
        }
        saveConfig();
        renderEnergyMeter(topTabsHtml, brandKey, slaveId);
      });
    });

    document.getElementById("em-save").onclick = () => {
      brand.registersBySlave = brand.registersBySlave || {};
      brand.registersBySlave[regKey] = deepClone(workingRows);
      for (let i = 0; i < workingRows.length; i++) {
        const r = workingRows[i];
        if (
          r.process_min !== "" &&
          r.process_max !== "" &&
          Number(r.process_min) >= Number(r.process_max)
        ) {
          alert(`Row ${i + 1}: Process Min must be less than Process Max`);
          return;
        }
      }

      saveConfig();
      flashTick("em-tick");
    };
  } else {
    document.getElementById("em-save").onclick = () => {
      for (let i = 0; i < workingRows.length; i++) {
        const r = workingRows[i];
        if (
          r.process_min !== "" &&
          r.process_max !== "" &&
          Number(r.process_min) >= Number(r.process_max)
        ) {
          alert(`Row ${i + 1}: Process Min must be less than Process Max`);
          return;
        }
      }

      saveConfig();
      flashTick("em-tick");
    };
  }
}

function rowHtml(r, i) {
  return `
        <tr>
          <td>${i + 1}</td>
          <td><input type="text" value="${esc(
            r.name
          )}" data-field="name" data-index="${i}" style="width:180px;"></td>
          <td><input type="number" value="${numOrEmpty(
            r.start
          )}" data-field="start" data-index="${i}" style="width:90px;"></td>
          <td><input type="number" value="${numOrEmpty(
            r.offset,
            0
          )}" data-field="offset" data-index="${i}" style="width:70px;"></td>
          <td>
            <select data-field="type" data-index="${i}">
              <option value="Holding Register" ${
                r.type === "Holding Register" ? "selected" : ""
              }>Holding Register</option>
              <option value="Input Register" ${
                r.type === "Input Register" ? "selected" : ""
              }>Input Register</option>
            </select>
          </td>
          <td>
            <select data-field="conversion" data-index="${i}">
              <option value="Raw Hex" ${
                r.conversion === "Raw Hex" ? "selected" : ""
              }>Raw Hex</option>
              <option value="Integer" ${
                r.conversion === "Integer" ? "selected" : ""
              }>Integer</option>
              <option value="Double" ${
                r.conversion === "Double" ? "selected" : ""
              }>Double</option>
              <option value="Float: Big Endian" ${
                r.conversion === "Float: Big Endian" ? "selected" : ""
              }>Float: Big Endian</option>
            </select>
          </td>
          <td><input type="number" value="${numOrEmpty(
            r.length,
            2
          )}" data-field="length" data-index="${i}" style="width:70px;"></td>
          <td>
            <div style="display:flex; gap:6px;">
              <input type="number"
                placeholder="Min"
                value="${numOrEmpty(r.process_min)}"
                data-field="process_min"
                data-index="${i}"
                style="width:70px;">
              <input type="number"
                placeholder="Max"
                value="${numOrEmpty(r.process_max)}"
                data-field="process_max"
                data-index="${i}"
                style="width:70px;">
            </div>
          </td>

          <td><input type="checkbox" ${
            r.enabled ? "checked" : ""
          } data-field="enabled" data-index="${i}"></td>
          <td style="text-align:center;">
            <button type="button" class="button em-reg-remove" data-index="${i}" title="Remove">×</button>
          </td>
        </tr>
      `;
}
function rerenderRegsTable(rows) {
  const table = document.getElementById("em-regs-table");
  if (!table) return;
  table.innerHTML = `
        <tr>
          <th>S.No</th>
          <th>Name</th>
          <th>Start Address</th>
          <th>Offset</th>
          <th>Type</th>
          <th>Conversion</th>
          <th>Length</th>
          <th>Process Range</th>
          <th>Enabled</th>
          <th>Remove</th>
        </tr>
        ${rows.map((r, i) => rowHtml(r, i)).join("")}
      `;
}

// Modals
function brandAddModal() {
  return `
        <div id="em-brand-modal" class="modal" aria-modal="true" role="dialog" aria-labelledby="brand-title" style="display:none;">
          <div class="modal-content">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <h3 id="brand-title" style="margin:0;">Add Brand</h3>
              <button id="em-brand-modal-close" class="button" aria-label="Close">×</button>
            </div>
            <div style="margin-top:10px;">
              <label>Brand Name: <input id="em-brand-name" type="text" placeholder="e.g., Schneider EM6436H" style="width:260px;"></label>
              <br><br>
              <button id="em-brand-save" class="button-primary">Save</button>
            </div>
          </div>
        </div>
      `;
}
function openBrandModal(onSave) {
  const container = document.createElement("div");
  container.innerHTML = brandAddModal();
  document.body.appendChild(container.firstElementChild);
  const m = document.getElementById("em-brand-modal");
  m.style.display = "flex";
  document.getElementById("em-brand-modal-close").onclick = () => {
    m.remove();
  };
  document.getElementById("em-brand-save").onclick = () => {
    const label = document.getElementById("em-brand-name").value.trim();
    if (!label) return;
    m.remove();
    onSave(label);
  };
}

function presetNameModal() {
  return `
        <div id="em-preset-modal" class="modal" aria-modal="true" role="dialog" aria-labelledby="preset-title" style="display:none;">
          <div class="modal-content">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <h3 id="preset-title" style="margin:0;">Save Preset</h3>
              <button id="em-preset-modal-close" class="button" aria-label="Close">×</button>
            </div>
            <div style="margin-top:10px;">
              <label>Name: <input id="em-preset-name" type="text" placeholder="e.g., Site A v1" style="width:220px;"></label>
              <br><br>
              <button id="em-preset-save" class="button-primary">Save</button>
            </div>
          </div>
        </div>
      `;
}
function openPresetNameModal(onSave) {
  const container = document.createElement("div");
  container.innerHTML = presetNameModal();
  document.body.appendChild(container.firstElementChild);
  const m = document.getElementById("em-preset-modal");
  m.style.display = "flex";
  document.getElementById("em-preset-modal-close").onclick = () => {
    m.remove();
  };
  document.getElementById("em-preset-save").onclick = () => {
    const name = document.getElementById("em-preset-name").value.trim();
    if (!name) return;
    m.remove();
    onSave(name);
  };
}

/***********************
 * Modbus RTU Panel (independent, renamed helpers)
 * - Uses config.ModbusRTU.PLC for persistence only.
 * - Initializes with JSON placeholder if empty.
 * - UI state lives in rtu_plc_configurations.
 * - Function names changed as requested (renderModBusRTUPanel unchanged).
 ***********************/

// Deep copy helper (structuredClone preferred with JSON fallback)
function rtuDeepCopy(obj) {
  try {
    if (typeof structuredClone === "function") return structuredClone(obj);
  } catch (e) {}
  return JSON.parse(JSON.stringify(obj));
}

// RTU-only editor state
let rtu_plc_configurations = [];

let rtuLoaded = 0;

// Exact JSON placeholder (Siemens RTU) requested
const RTU_JSON_PLACEHOLDER = [
  {
    plcType: "Siemens",
    isExpanded: true,
    PLC: {
      cred: {
        port:
          navigator.platform && navigator.platform.startsWith("Win")
            ? "COM3"
            : "/dev/ttyUSB0",
        baudrate: 9600,
        parity: "N",
        stopbits: 1,
        slave_id: 1,
      },
      address_access: {
        read: [
          {
            type: "holding_register",
            read: true,
            content: "Bentonite_actual_value",
            address: 40001,
            length: 1,
            datatype: "int16",
            write: false,
          },
          {
            type: "holding_register",
            read: true,
            content: "Bentonite_set_value",
            address: 40002,
            length: 1,
            datatype: "int16",
            write: false,
          },
        ],
        write: [
          {
            type: "coil",
            content: "Start_Motor",
            address: 1,
            value_to_write: 0,
          },
        ],
      },
      Database: { table_name: "scada_data" },
    },
  },
];

// Create a blank RTU entry for Add PLC
function makeBlankRtuPlcEntry() {
  return {
    plcType: "Other",
    isExpanded: true,
    PLC: {
      cred: {
        port:
          navigator.platform && navigator.platform.startsWith("Win")
            ? "COM3"
            : "/dev/ttyUSB0",
        baudrate: 9600,
        parity: "N",
        stopbits: 1,
        slave_id: 1,
      },
      address_access: { read: [], write: [] },
      Database: { table_name: "" },
    },
  };
}

// Load/Save RTU configs
function loadRtuPlcState() {
  const stored = config?.ModbusRTU?.PLC;
  console.log(config.ModbusRTU.PLC);
  const usePlaceholder = !(Array.isArray(stored) && stored.length > 0);
  const source = usePlaceholder ? RTU_JSON_PLACEHOLDER : stored;
  if (usePlaceholder) {
    config.ModbusRTU = config.ModbusRTU || {};
    config.ModbusRTU.PLC = rtuDeepCopy(RTU_JSON_PLACEHOLDER);
  }
  rtu_plc_configurations = rtuDeepCopy(source);
  // Normalize so bindings don't crash
  rtu_plc_configurations = rtu_plc_configurations.map((item) => ({
    plcType: item?.plcType || "Other",
    isExpanded: item?.isExpanded !== false,
    PLC: item?.PLC || {
      cred: {},
      address_access: { read: [], write: [] },
      Database: { table_name: "" },
    },
  }));
}

function saveRtuPlcState() {
  config.ModbusRTU = config.ModbusRTU || {};
  config.ModbusRTU.PLC = rtuDeepCopy(rtu_plc_configurations);
  if (typeof saveConfig === "function") saveConfig();
}

// Panel init hook (call this to open RTU PLC tab)
function initRtuPlcPanel() {
  loadRtuPlcState();
  renderModBusRTUPanel("PLC");
}

// HTML render
function rtuPlcInnerHtml() {
  if (rtuLoaded == 0) {
    loadRtuPlcState();
    rtuLoaded = 1;
  }
  const plcConfigs = Array.isArray(rtu_plc_configurations)
    ? rtu_plc_configurations
    : [];
  return `
        <div class="split-toolbar" style="margin-bottom:8px;">
          <div class="soft">Configure Modbus RTU PLC endpoints (independent from TCP).</div>
          <div>
            <button type="button" id="add-plc-entry" class="button">＋ Add PLC</button>
            <button type="button" id="save-all-configs" class="button-primary">Save All Configurations</button>
          </div>
        </div>
        <div id="plc-entries-container">
          ${plcConfigs
            .map((plc, index) => renderRtuPlcEntry(plc, index))
            .join("")}
        </div>
      `;
}

// Event bindings
function bindRtuPlcInnerEvents() {
  const addBtn = document.getElementById("add-plc-entry");
  const saveBtn = document.getElementById("save-all-configs");

  if (addBtn)
    addBtn.addEventListener("click", () => {
      if (!Array.isArray(rtu_plc_configurations)) rtu_plc_configurations = [];
      rtu_plc_configurations.push(makeBlankRtuPlcEntry());
      renderModBusRTUPanel("PLC");
    });

  if (saveBtn)
    saveBtn.addEventListener("click", () => {
      saveRtuPlcState();
      alert("All Modbus RTU PLC configurations have been saved.");
    });

  // Bind entries after render; guard indices
  const entryEls = document.querySelectorAll(".plc-entry");
  entryEls.forEach((el) => {
    const idx = parseInt(el.getAttribute("data-index"), 10);
    if (!Number.isInteger(idx)) return;
    const item = rtu_plc_configurations[idx];
    if (!item || typeof item !== "object") return;
    if (!("plcType" in item)) item.plcType = "Other";
    if (!("PLC" in item))
      item.PLC = {
        cred: {},
        address_access: { read: [], write: [] },
        Database: { table_name: "" },
      };
    bindRtuEventsForPlcEntry(idx);
  });
}

// Single entry card
function renderRtuPlcEntry(plc, index) {
  const portDisplay = plc?.PLC?.cred?.port || "Port Not Set";
  const sidDisplay =
    plc?.PLC?.cred?.slave_id != null ? `, SID ${plc.PLC.cred.slave_id}` : "";
  const isExpanded = plc.isExpanded !== false;
  const isOther = plc.plcType === "Other";
  const isAllenBradley = plc.plcType === "Allen Bradley";

  return `
        <div class="card plc-entry" data-index="${index}">
          <div class="accordion-h">
            <strong>${plc.plcType} - ${portDisplay}${sidDisplay}</strong>
            <div>
              <button type="button" class="button toggle-plc-details" title="Toggle">${
                isExpanded ? "−" : "+"
              }</button>
              <button type="button" class="button remove-plc-entry" title="Remove">×</button>
            </div>
          </div>
          <div class="plc-details" style="${isExpanded ? "" : "display:none;"}">
            <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; margin:12px 0;">
              <label>PLC Type:
                <select class="plc-type-select">
                  <option value="Other" ${
                    isOther ? "selected" : ""
                  }>Other</option>
                  <option value="Siemens" ${
                    plc.plcType === "Siemens" ? "selected" : ""
                  }>Siemens</option>
                  <option value="Allen Bradley" ${
                    isAllenBradley ? "selected" : ""
                  }>Allen Bradley</option>
                </select>
              </label>
              <label class="plc-driver-label" style="${
                isAllenBradley ? "" : "display:none;"
              }">
                Driver Type:
                <select class="plc-driver-select">
                  <option value="logix" ${
                    plc.PLC?.cred?.driver === "logix" ? "selected" : ""
                  }>Logix</option>
                  <option value="slc" ${
                    plc.PLC?.cred?.driver === "slc" ? "selected" : ""
                  }>SLC</option>
                </select>
              </label>
              <label class="plc-name-label" style="${
                isOther ? "display:none;" : ""
              }">
                Table Name:
                <input type="text" class="plc-name-input" value="${
                  plc.PLC?.Database?.table_name || ""
                }" />
              </label>
            </div>
            <div class="plc-form-container">
              ${renderRtuFormForType(plc.plcType, plc.PLC, index)}
            </div>
          </div>
        </div>
      `;
}

function bindRtuEventsForPlcEntry(index) {
  if (!Array.isArray(rtu_plc_configurations)) return;
  const cfg = rtu_plc_configurations[index];
  if (!cfg) return;
  if (!cfg.PLC)
    cfg.PLC = {
      cred: {},
      address_access: { read: [], write: [] },
      Database: { table_name: "" },
    };
  if (!("plcType" in cfg)) cfg.plcType = "Other";

  const entryElement = document.querySelector(
    `.plc-entry[data-index='${index}']`
  );
  if (!entryElement) return;

  const toggleBtn = entryElement.querySelector(".toggle-plc-details");
  if (toggleBtn)
    toggleBtn.addEventListener("click", () => {
      const item = rtu_plc_configurations[index];
      if (!item) return;
      item.isExpanded = !item.isExpanded;
      renderModBusRTUPanel("PLC");
    });

  const typeSel = entryElement.querySelector(".plc-type-select");
  if (typeSel)
    typeSel.addEventListener("change", (e) => {
      const item = rtu_plc_configurations[index];
      if (!item) return;
      item.plcType = e.target.value;
      item.PLC = item.PLC || {
        cred: {},
        address_access: { read: [], write: [] },
        Database: { table_name: "" },
      };
      renderModBusRTUPanel("PLC");
    });

  const driverSelect = entryElement.querySelector(".plc-driver-select");
  if (driverSelect) {
    driverSelect.addEventListener("change", (e) => {
      const item = rtu_plc_configurations[index];
      if (!item) return;
      const plcConfig = (item.PLC ||= {
        cred: {},
        address_access: { read: [], write: [] },
        Database: { table_name: "" },
      });
      (plcConfig.cred ||= {}).driver = e.target.value;
      renderModBusRTUPanel("PLC");
    });
  }

  const nameInput = entryElement.querySelector(".plc-name-input");
  if (nameInput) {
    nameInput.addEventListener("input", (e) => {
      const item = rtu_plc_configurations[index];
      if (!item) return;
      const plcConfig = (item.PLC ||= {
        cred: {},
        address_access: { read: [], write: [] },
        Database: { table_name: "" },
      });
      (plcConfig.Database ||= {}).table_name = e.target.value;
    });
  }

  const removeBtn = entryElement.querySelector(".remove-plc-entry");
  if (removeBtn) {
    removeBtn.addEventListener("click", () => {
      if (!confirm("Remove this PLC configuration?")) return;
      rtu_plc_configurations.splice(index, 1);
      renderModBusRTUPanel("PLC");
    });
  }

  const plcType = rtu_plc_configurations[index]?.plcType || "Other";
  if (plcType === "Siemens") bindRtuSiemensFormEvents(index);
  else if (plcType === "Allen Bradley") bindRtuAllenBradleyFormEvents(index);
}

// Type switcher
function renderRtuFormForType(type, plcData, index) {
  if (type === "Siemens") return renderRtuSiemensForm(plcData, index);
  if (type === "Allen Bradley")
    return renderRtuAllenBradleyForm(plcData, index);
  return ""; // "Other" shows nothing
}

// Siemens RTU defaults (requested JSON shape)
function ensureRtuSiemensDefaults(plc) {
  const p = plc || {};
  p.cred = p.cred || {};
  p.cred.port =
    p.cred.port ||
    (navigator.platform && navigator.platform.startsWith("Win")
      ? "COM3"
      : "/dev/ttyUSB0");
  p.cred.baudrate = p.cred.baudrate || 9600;
  p.cred.parity = p.cred.parity || "N";
  p.cred.stopbits = p.cred.stopbits || 1;
  p.cred.slave_id = p.cred.slave_id != null ? p.cred.slave_id : 1;
  p.Database = p.Database || { table_name: "" };
  p.address_access = p.address_access || {
    read: [],
    write: [{ type: "coil", content: "", address: 1, value_to_write: 0 }],
  };
  return p;
}

function renderRtuSiemensForm(plcData, index) {
  const plcConfig = ensureRtuSiemensDefaults(plcData || {});
  const c = plcConfig.cred;
  const readRows = (plcConfig.address_access.read || [])
    .map((item, rowIndex) => renderRtuSiemensRow(item, rowIndex))
    .join("");
  const writeRows = (plcConfig.address_access.write || [])
    .map(
      (item, rowIndex) => `
        <tr data-write-index="${rowIndex}">
          <td><input type="text" data-write-field="content" value="${
            item.content || ""
          }" placeholder="Tag"></td>
          <td><input type="number" data-write-field="address" value="${
            item.address ?? 1
          }" min="0"></td>
          <td><input type="number" data-write-field="value_to_write" value="${
            item.value_to_write ?? 0
          }" step="1" min="0" max="1"></td>
          <td><button type="button" class="button remove-siemens-write-row">-</button></td>
        </tr>
      `
    )
    .join("");

  return `
        <form id="siemens-form-${index}">
          <fieldset><legend>RTU Credentials</legend>
            <label>Serial Port: <input type="text" data-cred-field="port" value="${
              c.port
            }"></label>
            <label>Baudrate: <input type="number" data-cred-field="baudrate" value="${
              c.baudrate
            }" min="1200" max="115200" step="600"></label>
            <label>Parity:
              <select data-cred-field="parity">
                ${["N", "E", "O"]
                  .map(
                    (p) =>
                      `<option value="${p}" ${
                        c.parity === p ? "selected" : ""
                      }>${p}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Stop Bits:
              <select data-cred-field="stopbits">
                ${[1, 2]
                  .map(
                    (s) =>
                      `<option value="${s}" ${
                        c.stopbits == s ? "selected" : ""
                      }>${s}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Slave ID: <input type="number" data-cred-field="slave_id" value="${
              c.slave_id
            }" min="1" max="247"></label>
          </fieldset>

          <fieldset><legend>Read Items</legend>
            <table>
              <thead>
                <tr>
                  <th>Type</th><th>Content</th><th>Address</th><th>Length</th><th>Datatype</th><th>Read</th><th>Write</th><th>Remove</th>
                </tr>
              </thead>
              <tbody class="siemens-read-tbody">${readRows}</tbody>
            </table>
            <button type="button" class="button add-siemens-read-row">＋ Add Read Item</button>
          </fieldset>

          <fieldset style="margin-top:12px;"><legend>Write Coils</legend>
            <table>
              <thead>
                <tr><th>Content</th><th>Address</th><th>Value</th><th>Remove</th></tr>
              </thead>
              <tbody class="siemens-write-tbody">${writeRows}</tbody>
            </table>
            <button type="button" class="button add-siemens-write-coil">＋ Add Write Coil</button>
          </fieldset>

          <label style="margin-top:8px; display:block;">
            Data Reading Frequency (secs):
            <input type="number" data-freq-field="data_reading_freq(in secs)" value="${
              plcConfig["data_reading_freq(in secs)"] || 180
            }" step="0.1">
          </label>
        </form>
      `;
}

function renderRtuSiemensRow(item, rowIndex) {
  return `
        <tr data-row-index="${rowIndex}">
          <td>
            <select data-field="type">
              ${["holding_register", "input_register", "coil", "discrete_input"]
                .map(
                  (t) =>
                    `<option value="${t}" ${
                      item.type === t ? "selected" : ""
                    }>${t}</option>`
                )
                .join("")}
            </select>
          </td>
          <td><input type="text" data-field="content" value="${
            item.content || ""
          }" placeholder="Tag"></td>
          <td><input type="number" data-field="address" value="${
            item.address || 0
          }" placeholder="40001"></td>
          <td><input type="number" data-field="length" value="${
            item.length || 1
          }" min="1"></td>
          <td>
            <select data-field="datatype">
              ${["int", "real", "dint", "bool"]
                .map(
                  (d) =>
                    `<option value="${d}" ${
                      item.datatype === d ? "selected" : ""
                    }>${d.toUpperCase()}</option>`
                )
                .join("")}
            </select>
          </td>
          <td><input type="checkbox" data-field="read" ${
            item.read !== false ? "checked" : ""
          }></td>
          <td><input type="checkbox" data-field="write" ${
            item.write ? "checked" : ""
          }></td>
          <td><button type="button" class="button remove-siemens-row">-</button></td>
        </tr>
      `;
}

function bindRtuSiemensFormEvents(index) {
  const form = document.getElementById(`siemens-form-${index}`);
  if (!form) return;
  const plcConfig = (rtu_plc_configurations[index].PLC ||= {
    cred: {},
    address_access: { read: [], write: [] },
    Database: { table_name: "" },
  });
  ensureRtuSiemensDefaults(plcConfig);

  // Inputs: credentials, frequency, read rows, write rows
  form.addEventListener("input", (e) => {
    const key = e.target.dataset.credField;
    if (key) {
      const numeric = ["baudrate", "stopbits", "slave_id"];
      (plcConfig.cred ||= {})[key] = numeric.includes(key)
        ? Number(e.target.value)
        : e.target.value;
      return;
    }
    if (e.target.dataset.freqField === "data_reading_freq(in secs)") {
      const val = parseFloat(e.target.value);
      if (!isNaN(val)) plcConfig["data_reading_freq(in secs)"] = val;
      return;
    }
    // Read row updates
    const readRow = e.target.closest("tbody.siemens-read-tbody tr");
    if (readRow) {
      const rowIndex = parseInt(readRow.dataset.rowIndex, 10);
      const field = e.target.dataset.field;
      let value =
        e.target.type === "checkbox" ? e.target.checked : e.target.value;
      if (e.target.type === "number" && !isNaN(parseFloat(value)))
        value = parseFloat(value);
      plcConfig.address_access.read ||= [];
      plcConfig.address_access.read[rowIndex] ||= {};
      plcConfig.address_access.read[rowIndex][field] = value;
      return;
    }
    // Write row updates
    const writeRow = e.target.closest("tbody.siemens-write-tbody tr");
    if (writeRow) {
      const wIndex = parseInt(writeRow.dataset.writeIndex, 10);
      const wField = e.target.dataset.writeField;
      let wValue =
        e.target.type === "checkbox" ? e.target.checked : e.target.value;
      if (e.target.type === "number" && !isNaN(parseFloat(wValue)))
        wValue = parseFloat(wValue);
      plcConfig.address_access.write ||= [];
      plcConfig.address_access.write[wIndex] ||= {
        type: "coil",
        content: "",
        address: 1,
        value_to_write: 0,
      };
      plcConfig.address_access.write[wIndex][wField] = wValue;
      return;
    }
  });

  // Delegated clicks: add/remove read, add/remove write
  form.addEventListener("click", (e) => {
    if (e.target.classList.contains("add-siemens-read-row")) {
      (plcConfig.address_access.read ||= []).push({
        type: "holding_register",
        read: true,
        content: "",
        address: 40001,
        length: 1,
        datatype: "int16",
        write: false,
      });
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("remove-siemens-row")) {
      const rowIndex = parseInt(e.target.closest("tr").dataset.rowIndex, 10);
      plcConfig.address_access.read.splice(rowIndex, 1);
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("add-siemens-write-coil")) {
      (plcConfig.address_access.write ||= []).push({
        type: "coil",
        content: "",
        address: 1,
        value_to_write: 0,
      });
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("remove-siemens-write-row")) {
      const wIndex = parseInt(e.target.closest("tr").dataset.writeIndex, 10);
      plcConfig.address_access.write.splice(wIndex, 1);
      renderModBusRTUPanel("PLC");
      return;
    }
  });
}

// Allen Bradley RTU
function renderRtuAllenBradleyForm(plcData, index) {
  const plcConfig = ensureRtuSiemensDefaults(plcData || {});
  const c = plcConfig.cred || {};
  if (!c.driver) c.driver = "logix";
  if (!Array.isArray(plcConfig.address_of_value))
    plcConfig.address_of_value = [""];

  const isLogix = c.driver === "logix";
  const readRows = (plcConfig.address_access.read || [])
    .map((item, rowIndex) => renderRtuSiemensRow(item, rowIndex))
    .join("");
  const writeRows = (plcConfig.address_access.write || [])
    .map(
      (item, rowIndex) => `
        <tr data-write-index="${rowIndex}">
          <td><input type="text" data-write-field="content" value="${
            item.content || ""
          }" placeholder="Tag"></td>
          <td><input type="number" data-write-field="address" value="${
            item.address ?? 1
          }" min="0"></td>
          <td><input type="number" data-write-field="value_to_write" value="${
            item.value_to_write ?? 0
          }" step="1" min="0" max="1"></td>
          <td><button type="button" class="button remove-siemens-write-row">-</button></td>
        </tr>
      `
    )
    .join("");

  const addressChain = `
        <div class="address-value-chain" style="display:${
          isLogix ? "block" : "none"
        }; margin-top:16px;">
          <div class="soft" style="margin-bottom:8px;">Enter the tag path segments for Logix (e.g., Program:MainProgram → MyAOI → PV).</div>
          <div class="address-chain" style="display:flex; align-items:center; flex-wrap:wrap; gap:8px; padding:12px; border:1px solid #ddd; border-radius:6px; background:#fafafa;">
            ${(plcConfig.address_of_value || [""])
              .map(
                (value, nodeIndex) => `
              <div style="display:flex; align-items:center; gap:6px;">
                <input type="text" class="ab-path-node" data-node-index="${nodeIndex}" value="${
                  value || ""
                }" placeholder="Segment ${
                  nodeIndex + 1
                }" style="padding:6px; border:1px solid #ccc; border-radius:4px; font-family:monospace; width:160px;">
                <button type="button" class="button ab-remove-node" data-node-index="${nodeIndex}" style="display:${
                  (plcConfig.address_of_value || []).length > 1
                    ? "inline-block"
                    : "none"
                };">×</button>
                ${
                  nodeIndex < (plcConfig.address_of_value || []).length - 1
                    ? '<span style="margin:0 4px;">→</span>'
                    : ""
                }
              </div>
            `
              )
              .join("")}
            <button type="button" class="button ab-add-node">＋</button>
          </div>
        </div>
      `;

  return `
        <form id="ab-form-${index}">
          <fieldset><legend>RTU Credentials</legend>
            <label>Serial Port: <input type="text" data-cred-field="port" value="${
              c.port ||
              (navigator.platform && navigator.platform.startsWith("Win")
                ? "COM3"
                : "/dev/ttyUSB0")
            }"></label>
            <label>Baudrate: <input type="number" data-cred-field="baudrate" value="${
              c.baudrate || 9600
            }" min="1200" max="115200" step="600"></label>
            <label>Parity:
              <select data-cred-field="parity">
                ${["N", "E", "O"]
                  .map(
                    (p) =>
                      `<option value="${p}" ${
                        c.parity === p ? "selected" : ""
                      }>${p}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Stop Bits:
              <select data-cred-field="stopbits">
                ${[1, 2]
                  .map(
                    (s) =>
                      `<option value="${s}" ${
                        c.stopbits == s ? "selected" : ""
                      }>${s}</option>`
                  )
                  .join("")}
              </select>
            </label>
            <label>Slave ID: <input type="number" data-cred-field="slave_id" value="${
              c.slave_id ?? 1
            }" min="1" max="247"></label>
            <label>Driver:
              <select data-cred-field="driver" class="ab-driver-select">
                <option value="logix" ${
                  c.driver === "logix" ? "selected" : ""
                }>Logix</option>
                <option value="slc" ${
                  c.driver === "slc" ? "selected" : ""
                }>SLC</option>
              </select>
            </label>
          </fieldset>

          <fieldset><legend>Read Items</legend>
            <table>
              <thead>
                <tr>
                  <th>Type</th><th>Content</th><th>Address</th><th>length</th><th>Datatype</th><th>Read</th><th>Write</th><th>Remove</th>
                </tr>
              </thead>
              <tbody class="siemens-read-tbody">${readRows}</tbody>
            </table>
            <button type="button" class="button add-siemens-read-row">＋ Add Read Item</button>
          </fieldset>

          <fieldset style="margin-top:12px;"><legend>Write Coils</legend>
            <table>
              <thead>
                <tr><th>Content</th><th>Address</th><th>Value</th><th>Remove</th></tr>
              </thead>
            <tbody class="siemens-write-tbody">${writeRows}</tbody>
            </table>
            <button type="button" class="button add-siemens-write-coil">＋ Add Write Coil</button>
          </fieldset>

          <fieldset><legend>Address of Value</legend>
            ${addressChain}
          </fieldset>
        </form>
      `;
}

function bindRtuAllenBradleyFormEvents(index) {
  const form = document.getElementById(`ab-form-${index}`);
  if (!form) return;
  const plcConfig = (rtu_plc_configurations[index].PLC ||= {
    cred: {},
    address_access: { read: [], write: [] },
    Database: { table_name: "" },
  });
  ensureRtuSiemensDefaults(plcConfig);

  // Inputs: credentials/driver, read rows, write rows, address chain nodes
  form.addEventListener("input", (e) => {
    const key = e.target.dataset.credField;
    if (key) {
      const numeric = ["baudrate", "stopbits", "slave_id"];
      (plcConfig.cred ||= {})[key] = numeric.includes(key)
        ? Number(e.target.value)
        : e.target.value;
      if (key === "driver") renderModBusRTUPanel("PLC");
      return;
    }
    // Address chain node edit
    if (e.target.classList.contains("ab-path-node")) {
      const nodeIndex = parseInt(e.target.dataset.nodeIndex, 10);
      if (!Array.isArray(plcConfig.address_of_value))
        plcConfig.address_of_value = [""];
      plcConfig.address_of_value[nodeIndex] = e.target.value;
      return;
    }
    // Read row updates
    const readRow = e.target.closest("tbody.siemens-read-tbody tr");
    if (readRow) {
      const rowIndex = parseInt(readRow.dataset.rowIndex, 10);
      const field = e.target.dataset.field;
      let value =
        e.target.type === "checkbox" ? e.target.checked : e.target.value;
      if (e.target.type === "number" && !isNaN(parseFloat(value)))
        value = parseFloat(value);
      plcConfig.address_access.read ||= [];
      plcConfig.address_access.read[rowIndex] ||= {};
      plcConfig.address_access.read[rowIndex][field] = value;
      return;
    }
    // Write row updates
    const writeRow = e.target.closest("tbody.siemens-write-tbody tr");
    if (writeRow) {
      const wIndex = parseInt(writeRow.dataset.writeIndex, 10);
      const wField = e.target.dataset.writeField;
      let wValue =
        e.target.type === "checkbox" ? e.target.checked : e.target.value;
      if (e.target.type === "number" && !isNaN(parseFloat(wValue)))
        wValue = parseFloat(wValue);
      plcConfig.address_access.write ||= [];
      plcConfig.address_access.write[wIndex] ||= {
        type: "coil",
        content: "",
        address: 1,
        value_to_write: 0,
      };
      plcConfig.address_access.write[wIndex][wField] = wValue;
      return;
    }
  });

  // Delegated clicks: add/remove read, add/remove write, add/remove path nodes
  form.addEventListener("click", (e) => {
    if (e.target.classList.contains("add-siemens-read-row")) {
      (plcConfig.address_access.read ||= []).push({
        type: "holding_register",
        read: true,
        content: "",
        address: 40001,
        length: 1,
        datatype: "int16",
        write: false,
      });
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("remove-siemens-row")) {
      const rowIndex = parseInt(e.target.closest("tr").dataset.rowIndex, 10);
      plcConfig.address_access.read.splice(rowIndex, 1);
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("add-siemens-write-coil")) {
      (plcConfig.address_access.write ||= []).push({
        type: "coil",
        content: "",
        address: 1,
        value_to_write: 0,
      });
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("remove-siemens-write-row")) {
      const wIndex = parseInt(e.target.closest("tr").dataset.writeIndex, 10);
      plcConfig.address_access.write.splice(wIndex, 1);
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("ab-add-node")) {
      (plcConfig.address_of_value ||= []).push("");
      renderModBusRTUPanel("PLC");
      return;
    }
    if (e.target.classList.contains("ab-remove-node")) {
      const nodeIndex = parseInt(e.target.dataset.nodeIndex, 10);
      if (
        Array.isArray(plcConfig.address_of_value) &&
        plcConfig.address_of_value.length > 1
      ) {
        plcConfig.address_of_value.splice(nodeIndex, 1);
        renderModBusRTUPanel("PLC");
      }
      return;
    }
  });
}

function transmitterInnerHtml() {
  const list = config.ModbusRTU.transmitters;
  return `
        <div class="split-toolbar" style="margin-bottom:8px;">
          <div class="soft">Manage Modbus RTU transmitters and mapped registers.</div>
          <div>
            <button id="tx-add" class="button">＋ Add</button>
            <button id="tx-save" class="button-primary">Save</button>
          </div>
        </div>
        <div id="tx-list">
          ${list.map((t, i) => txCard(t, i)).join("")}
        </div>
      `;
}

function bindTransmitterEvents() {
  const list = config.ModbusRTU.transmitters;
  document.getElementById("tx-add")?.addEventListener("click", () => {
    list.push({
      type: "RTU",
      name: "",
      serialPort: "/dev/ttyUSB0",
      baudRate: 9600,
      dataBits: 8,
      parity: "Even",
      stopBits: 1,
      unitId: 1,
      interval: 5,
      functionCode: 3, // 03=holding, 04=input
      registers: [],
    });
    renderModBusRTUPanel("Transmitter");
  });
  document.getElementById("tx-save")?.addEventListener("click", () => {
    // Optional validation: unitId 1..247, fc 3 or 4, count >=1
    saveConfig();
    alert("Transmitters saved.");
  });
  list.forEach((_, i) => bindTxCard(i));
}

function txCard(t, i) {
  const regs = (t.registers || []).map((r, ri) => regRow(r, i, ri)).join("");
  return `
        <div class="card" data-tx="${i}">
          <div class="split-toolbar">
            <b>${t.name || "(unnamed)"} <span class="soft">- RTU</span></b>
            <button class="button tx-remove">Remove</button>
          </div>

          <div style="display:flex; flex-wrap:wrap; gap:12px; margin-top:10px;">
            <label>Name: <input class="tx-name" type="text" value="${
              t.name || ""
            }"></label>
            <label>Serial Port: <input class="tx-serial" type="text" value="${
              t.serialPort || "/dev/ttyUSB0"
            }"></label>
            <label>Baud: <input class="tx-baud" type="number" min="1200" step="1" value="${
              t.baudRate || 9600
            }"></label>
            <label>Data Bits:
              <select class="tx-dbits">
                <option value="7" ${
                  t.dataBits === 7 ? "selected" : ""
                }>7</option>
                <option value="8" ${
                  t.dataBits === 8 ? "selected" : ""
                }>8</option>
              </select>
            </label>
            <label>Parity:
              <select class="tx-parity">
                <option value="None" ${
                  t.parity === "None" ? "selected" : ""
                }>None</option>
                <option value="Even" ${
                  t.parity === "Even" ? "selected" : ""
                }>Even</option>
                <option value="Odd" ${
                  t.parity === "Odd" ? "selected" : ""
                }>Odd</option>
              </select>
            </label>
            <label>Stop Bits:
              <select class="tx-sbits">
                <option value="1" ${
                  t.stopBits === 1 ? "selected" : ""
                }>1</option>
                <option value="2" ${
                  t.stopBits === 2 ? "selected" : ""
                }>2</option>
              </select>
            </label>
            <label>Unit ID: <input class="tx-unit" type="number" min="1" max="247" value="${
              t.unitId || 1
            }"></label>
            <label>Interval (s): <input class="tx-int" type="number" min="1" value="${
              t.interval || 5
            }"></label>
            <label>Function:
              <select class="tx-fc">
                <option value="3" ${
                  t.functionCode === 3 ? "selected" : ""
                }>03 - Read Holding</option>
                <option value="4" ${
                  t.functionCode === 4 ? "selected" : ""
                }>04 - Read Input</option>
              </select>
            </label>
          </div>

          <div style="margin-top:12px;">
            <div class="split-toolbar">
              <span class="soft">Registers</span>
              <button class="button tx-reg-add">＋ Add Register</button>
            </div>
            <div class="tx-reg-list">
              ${
                regs ||
                `<div class="soft" style="margin-top:6px;">No registers yet.</div>`
              }
            </div>
          </div>
        </div>
      `;
}

function regRow(r, ti, ri) {
  return `
        <div class="row" data-tx="${ti}" data-reg="${ri}" style="display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; align-items:center;">
          <label style="min-width:180px;">Name: <input class="reg-name" type="text" value="${
            r.name || ""
          }"></label>
          <label>Address: <input class="reg-addr" type="number" min="0" value="${
            Number.isFinite(r.address) ? r.address : 0
          }"></label>
          <label>Count: <input class="reg-count" type="number" min="1" value="${
            r.count || 1
          }"></label>
          <label>Datatype:
            <select class="reg-type">
              <option value="int16" ${
                r.datatype === "int16" ? "selected" : ""
              }>int16</option>
              <option value="uint16" ${
                r.datatype === "uint16" ? "selected" : ""
              }>uint16</option>
              <option value="int32" ${
                r.datatype === "int32" ? "selected" : ""
              }>int32</option>
              <option value="uint32" ${
                r.datatype === "uint32" ? "selected" : ""
              }>uint32</option>
              <option value="float32" ${
                r.datatype === "float32" ? "selected" : ""
              }>float32</option>
            </select>
          </label>
          <label>Scaling: <input class="reg-scale" type="number" step="any" value="${
            r.scaling ?? 1.0
          }"></label>
          <label>Unit: <input class="reg-unit" type="text" value="${
            r.unit || ""
          }"></label>
          <button class="button tx-reg-remove">Remove</button>
        </div>
      `;
}

function bindTxCard(i) {
  const card = document.querySelector(`.card[data-tx="${i}"]`);
  if (!card) return;

  // Remove transmitter
  card.querySelector(".tx-remove").onclick = () => {
    if (!confirm("Remove this transmitter?")) return;
    config.ModbusRTU.transmitters.splice(i, 1);
    renderModBusRTUPanel("Transmitter");
  };

  // Add register
  card.querySelector(".tx-reg-add").onclick = () => {
    const t = config.ModbusRTU.transmitters[i];
    t.registers = t.registers || [];
    // sensible default for float32 holding reg at address 0
    t.registers.push({
      name: "",
      address: 0,
      count: 2,
      datatype: "float32",
      scaling: 1.0,
      unit: "",
    });
    renderModBusRTUPanel("Transmitter");
  };

  // Input handlers
  card.addEventListener("input", (e) => {
    const t = config.ModbusRTU.transmitters[i];
    if (e.target.classList.contains("tx-name")) t.name = e.target.value;
    if (e.target.classList.contains("tx-serial")) t.serialPort = e.target.value;
    if (e.target.classList.contains("tx-baud")) {
      const v = parseInt(e.target.value, 10);
      t.baudRate = Number.isFinite(v)
        ? Math.max(1200, Math.min(115200, v))
        : 9600;
    }
    if (e.target.classList.contains("tx-dbits"))
      t.dataBits = parseInt(e.target.value, 10) || 8;
    if (e.target.classList.contains("tx-parity")) t.parity = e.target.value;
    if (e.target.classList.contains("tx-sbits"))
      t.stopBits = parseInt(e.target.value, 10) || 1;
    if (e.target.classList.contains("tx-unit")) {
      const v = parseInt(e.target.value, 10);
      t.unitId = Number.isFinite(v) ? Math.max(1, Math.min(247, v)) : 1;
    }
    if (e.target.classList.contains("tx-int")) {
      const v = parseFloat(e.target.value);
      t.interval = Number.isFinite(v) ? Math.max(1, v) : 5;
    }
    if (e.target.classList.contains("tx-fc")) {
      const v = parseInt(e.target.value, 10);
      t.functionCode = v === 3 || v === 4 ? v : 3; // constrain to 03/04
    }
    t.type = "RTU";
  });

  // Register row handlers (delegate)
  card.addEventListener("click", (e) => {
    if (e.target.classList.contains("tx-reg-remove")) {
      const row = e.target.closest(".row");
      if (!row) return;
      const ri = parseInt(row.getAttribute("data-reg"), 10);
      const t = config.ModbusRTU.transmitters[i];
      t.registers.splice(ri, 1);
      renderModBusRTUPanel("Transmitter");
    }
  });

  card.addEventListener("input", (e) => {
    const row = e.target.closest(".row");
    if (!row) return;
    if (!e.target.classList) return;
    const ri = parseInt(row.getAttribute("data-reg"), 10);
    const t = config.ModbusRTU.transmitters[i];
    const r = t.registers?.[ri];
    if (!r) return;

    if (e.target.classList.contains("reg-name")) r.name = e.target.value;
    if (e.target.classList.contains("reg-addr")) {
      const v = parseInt(e.target.value, 10);
      r.address = Number.isFinite(v) ? Math.max(0, v) : 0; // zero-based offset
    }
    if (e.target.classList.contains("reg-count")) {
      const v = parseInt(e.target.value, 10);
      r.count = Number.isFinite(v) ? Math.max(1, v) : 1;
    }
    if (e.target.classList.contains("reg-type")) {
      r.datatype = e.target.value; // int16|uint16|int32|uint32|float32
      // optional auto-count for common types
      if (
        r.datatype === "float32" ||
        r.datatype === "int32" ||
        r.datatype === "uint32"
      )
        r.count = 2;
      if (r.datatype === "int16" || r.datatype === "uint16") r.count = 1;
    }
    if (e.target.classList.contains("reg-scale")) {
      const v = parseFloat(e.target.value);
      r.scaling = Number.isFinite(v) ? v : 1.0;
    }
    if (e.target.classList.contains("reg-unit")) r.unit = e.target.value;
  });
}

// Bootstrapping example: call router to show default tab after load
document.addEventListener("DOMContentLoaded", () => {
  renderModBusRTUPanel("EnergyMeter");
});

function renderModbusTcpPanel() {
  if (!config.plc_configurations) {
    config.plc_configurations = [];
  }
  const plcConfigs = config.plc_configurations;

  const mainPanelHtml = `
        <div class="panel-header">Modbus TCP Configuration</div>
        <div id="plc-entries-container">
          ${plcConfigs.map((plc, index) => renderPlcEntry(plc, index)).join("")}
        </div>
        <button type="button" id="add-plc-entry" class="button-primary" style="margin-top: 16px;">+ Add PLC Configuration</button>
        <button type="button" id="save-all-configs" class="button-primary" style="margin-left: 16px;">Save All Configurations</button>
      `;

  document.getElementById("main-panel").innerHTML = mainPanelHtml;

  // --- Main Event Listeners ---
  document.getElementById("add-plc-entry").addEventListener("click", () => {
    plcConfigs.push({ plcType: "Other", PLC: {}, isExpanded: true }); // New entries are expanded by default
    renderModbusTcpPanel();
  });

  document.getElementById("save-all-configs").addEventListener("click", () => {
    saveConfig();
    alert("All PLC configurations have been saved!");
  });

  plcConfigs.forEach((plc, index) => bindEventsForPlcEntry(index));
}

// --- Functions for Rendering and Binding a Single PLC Entry ---

function renderPlcEntry(plc, index) {
  const ipDisplay =
    plc.PLC && plc.PLC.cred && plc.PLC.cred.ip ? plc.PLC.cred.ip : "Not Set";
  const isExpanded = plc.isExpanded !== false; // Default to expanded if not set
  const isOther = plc.plcType === "Other";
  const isAllenBradley = plc.plcType === "Allen Bradley";

  return `
        <div class="plc-entry" data-index="${index}" style="border: 1px solid #ccc; margin-bottom: 12px; border-radius: 4px;">
          <div class="plc-header" style="cursor: pointer; display: flex; justify-content: space-between; align-items: center; padding: 8px; background: #f0f0f0;">
            <strong>${plc.plcType} - ${ipDisplay}</strong>
            <button type="button" class="toggle-plc-details">${
              isExpanded ? "−" : "+"
            }</button>
          </div>
          <div class="plc-details" style="padding: 16px; ${
            isExpanded ? "display: block;" : "display: none;"
          }">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; gap: 8px;">
              
              <label>PLC Type:
                <select class="plc-type-select">
                  <option value="Other" ${
                    plc.plcType === "Other" ? "selected" : ""
                  }>Other</option>
                  <option value="Siemens" ${
                    plc.plcType === "Siemens" ? "selected" : ""
                  }>Siemens</option>
                  <option value="Allen Bradley" ${
                    plc.plcType === "Allen Bradley" ? "selected" : ""
                  }>Allen Bradley</option>
                </select>
              </label>

              <label class="plc-driver-label" style="${
                isAllenBradley ? "" : "display:none;"
              }">
                Driver Type:
                <select class="plc-driver-select">
                  <option value="logix" ${
                    plc.PLC?.cred?.driver === "logix" ? "selected" : ""
                  }>Logix</option>
                  <option value="slc" ${
                    plc.PLC?.cred?.driver === "slc" ? "selected" : ""
                  }>SLC</option>
                </select>
              </label>

              <label class="plc-name-label" style="${
                isOther ? "display:none;" : ""
              }">
                Table Name:
                <input type="text" class="plc-name-input" value="${
                  plc.PLC?.Database?.table_name || ""
                }" />
              </label>

              <button type="button" class="remove-plc-entry">Remove</button>
            </div>

            <div class="plc-form-container">
              ${renderFormForType(plc.plcType, plc.PLC, index)}
            </div>
          </div>
        </div>
      `;
}

function renderFormForType(type, plcData, index) {
  if (type === "Siemens") return renderSiemensForm(plcData, index);
  if (type === "Allen Bradley") return renderAllenBradleyForm(plcData, index);
  return "<div><em>Please select a PLC type to configure its settings.</em></div>";
}

function bindEventsForPlcEntry(index) {
  const entryElement = document.querySelector(
    `.plc-entry[data-index='${index}']`
  );
  if (!entryElement) return;

  entryElement.querySelector(".plc-header").addEventListener("click", (e) => {
    if (e.target.classList.contains("toggle-plc-details")) {
      // Toggle expanded state
      config.plc_configurations[index].isExpanded =
        !config.plc_configurations[index].isExpanded;
      renderModbusTcpPanel();
    }
  });

  entryElement
    .querySelector(".plc-type-select")
    .addEventListener("change", (e) => {
      config.plc_configurations[index].plcType = e.target.value;
      config.plc_configurations[index].PLC = {};
      renderModbusTcpPanel();
    });

  // Driver type selection for Allen Bradley
  const driverSelect = entryElement.querySelector(".plc-driver-select");
  if (driverSelect) {
    driverSelect.addEventListener("change", (e) => {
      const plcConfig = config.plc_configurations[index].PLC;
      if (!plcConfig.cred) plcConfig.cred = {};
      plcConfig.cred.driver = e.target.value;
    });
  }

  const nameInput = entryElement.querySelector(".plc-name-input");
  if (nameInput) {
    nameInput.addEventListener("input", (e) => {
      const plcConfig = config.plc_configurations[index].PLC;
      if (!plcConfig.Database) plcConfig.Database = {};
      plcConfig.Database.table_name = e.target.value;
    });
  }

  entryElement
    .querySelector(".remove-plc-entry")
    .addEventListener("click", () => {
      config.plc_configurations.splice(index, 1);
      renderModbusTcpPanel();
    });

  const plcType = config.plc_configurations[index].plcType;
  if (plcType === "Siemens") bindSiemensFormEvents(index);
  else if (plcType === "Allen Bradley") bindAllenBradleyFormEvents(index);
}
// --- Siemens-Specific Functions ---

// --- shared row renderer ---
function renderSiemensRow(item, rowIndex) {
  return `
            <tr data-row-index="${rowIndex}">
                <td><input type="text" data-field="content" value="${
                  item.content || ""
                }" placeholder="Tag Name"></td>
                <td>
                    <select data-field="storage">
                        ${["DB", "I", "Q", "M"]
                          .map(
                            (m) =>
                              `<option value="${m}" ${
                                item.storage === m ? "selected" : ""
                              }>${m}</option>`
                          )
                          .join("")}
                    </select>
                </td>
                <td><input type="number" data-field="DB_no" value="${
                  item.DB_no || ""
                }" placeholder="DB No."></td>
                <td><input type="number" data-field="address" value="${
                  item.address || ""
                }" placeholder="Address"></td>
                <td>
                    <select class="siemens-read-type" data-field="type">
                        ${["int", "real", "dint", "bool"]
                          .map(
                            (t) =>
                              `<option value="${t}" ${
                                item.type === t ? "selected" : ""
                              }>${t.toUpperCase()}</option>`
                          )
                          .join("")}
                    </select>
                </td>
                <td><input type="number" data-field="size" value="${
                  item.size || ""
                }" readonly></td>
                <td><input type="checkbox" data-field="read" ${
                  item.read !== false ? "checked" : ""
                }></td>
                <td><input type="checkbox" data-field="write" ${
                  item.write ? "checked" : ""
                }></td>
                <td class="value-cell" style="${
                  item.write ? "" : "display:none;"
                }">
                    <input type="number" data-field="value_to_write" value="${
                      item.value_to_write || 0
                    }" style="width:70px;">
                </td>
                <td><button type="button" class="remove-siemens-row">-</button></td>
            </tr>
        `;
}

function renderSiemensForm(plcData, index) {
  const plcConfig = plcData || {};
  if (!plcConfig.cred) plcConfig.cred = { ip: "192.168.0.1", slot: 2, rack: 0 };
  if (!plcConfig.Database) plcConfig.Database = { table_name: "" };
  if (!plcConfig.address_access) plcConfig.address_access = {};
  if (!plcConfig.address_access.read) plcConfig.address_access.read = [];
  if (
    !plcConfig.address_access.write ||
    !plcConfig.address_access.write.length
  ) {
    plcConfig.address_access.write = [
      { content: "", address: "", value_to_write: 0 },
    ];
  }

  return `
            <form id="siemens-form-${index}">
                <fieldset><legend>Credentials</legend>
                    <label>IP: <input type="text" data-cred-field="ip" value="${
                      plcConfig.cred.ip
                    }"></label>
                    <label>Rack: <input type="number" data-cred-field="rack" value="${
                      plcConfig.cred.rack
                    }"></label>
                    <label>Slot: <input type="number" data-cred-field="slot" value="${
                      plcConfig.cred.slot
                    }"></label>
                </fieldset>
                <fieldset><legend>Read/Write Operations</legend>
                    <table>
                        <thead>
                            <tr>
                                <th>Content</th><th>Storage</th><th>DB No.</th><th>Address</th>
                                <th>Type</th><th>Size</th><th>Read</th><th>Write</th>
                                <th class="value-header" style="display:none;">Value to Write</th>
                                <th>Remove</th>
                            </tr>
                        </thead>
                        <tbody class="siemens-read-tbody">
                            ${plcConfig.address_access.read
                              .map(renderSiemensRow)
                              .join("")}
                        </tbody>
                    </table>
                    <button type="button" class="add-siemens-read-row">+ Add Item</button>
                </fieldset>
                <label>
                    Data Reading Frequency (secs): 
                    <input type="number" data-freq-field="data_reading_freq(in secs)" 
                          value="${
                            plcConfig["data_reading_freq(in secs)"] || 180
                          }" step="0.1">
                </label>
            </form>
        `;
}

function bindSiemensFormEvents(index) {
  const form = document.getElementById(`siemens-form-${index}`);
  if (!form) return;
  const plcConfig = config.plc_configurations[index].PLC;

  const tbody = form.querySelector(".siemens-read-tbody");
  const header = form.querySelector(".value-header");

  const updateHeaderVisibility = () => {
    const anyWrite = plcConfig.address_access.read.some((r) => r.write);
    header.style.display = anyWrite ? "" : "none";
  };

  // --- Read/Write rows listener ---
  tbody.addEventListener("input", (e) => {
    const row = e.target.closest("tr");
    if (!row) return;
    const rowIndex = parseInt(row.dataset.rowIndex, 10);
    const field = e.target.dataset.field;
    if (!field) return;

    let value =
      e.target.type === "checkbox" ? e.target.checked : e.target.value;
    if (e.target.type === "number" && !isNaN(parseFloat(value)))
      value = parseFloat(value);

    const readItem = plcConfig.address_access.read[rowIndex];

    if (field !== "value_to_write") readItem[field] = value;

    if (field === "write") {
      const valCell = row.querySelector(".value-cell");
      if (valCell) valCell.style.display = value ? "" : "none";

      if (value) {
        let existing = plcConfig.address_access.write.find(
          (w) => w.content === readItem.content
        );
        if (!existing) {
          let empty = plcConfig.address_access.write.find(
            (w) => w.content === ""
          );
          if (empty) {
            empty.content = readItem.content || "";
            empty.address = readItem.address || "";
            empty.value_to_write = 0;
          } else {
            plcConfig.address_access.write.push({
              content: readItem.content || "",
              address: readItem.address || "",
              value_to_write: 0,
            });
          }
        }
      } else {
        plcConfig.address_access.write = plcConfig.address_access.write.filter(
          (w) => w.content !== readItem.content
        );
        if (!plcConfig.address_access.write.length) {
          plcConfig.address_access.write.push({
            content: "",
            address: "",
            value_to_write: 0,
          });
        }
      }
      updateHeaderVisibility();
    }

    if (field === "value_to_write") {
      let writeItem = plcConfig.address_access.write.find(
        (w) => w.content === readItem.content
      );
      if (writeItem) writeItem.value_to_write = value;
    }
  });

  // --- Add row ---
  form.querySelector(".add-siemens-read-row").addEventListener("click", () => {
    plcConfig.address_access.read.push({
      content: "",
      storage: "DB",
      DB_no: "",
      address: "",
      type: "int",
      size: 2,
      read: true,
      write: false,
      value_to_write: 0,
    });
    tbody.innerHTML = plcConfig.address_access.read
      .map(renderSiemensRow)
      .join("");
    updateHeaderVisibility();
  });

  // --- Remove row ---
  tbody.addEventListener("click", (e) => {
    if (!e.target.classList.contains("remove-siemens-row")) return;
    const row = e.target.closest("tr");
    const rowIndex = parseInt(row.dataset.rowIndex, 10);
    plcConfig.address_access.read.splice(rowIndex, 1);
    tbody.innerHTML = plcConfig.address_access.read
      .map(renderSiemensRow)
      .join("");
    updateHeaderVisibility();
  });

  // --- Frequency input listener ---
  const freqInput = form.querySelector(
    '[data-freq-field="data_reading_freq(in secs)"]'
  );
  if (freqInput) {
    freqInput.addEventListener("input", (e) => {
      const val = parseFloat(e.target.value);
      if (!isNaN(val)) plcConfig["data_reading_freq(in secs)"] = val;
    });
  }

  updateHeaderVisibility();
}

// --- Allen Bradley-Specific Functions ---

function renderAllenBradleyForm(plcData, index) {
  const plcConfig = plcData || {};
  if (!plcConfig.cred)
    plcConfig.cred = {
      driver: "logix",
      ip: "192.168.1.200",
      slot: 1,
      port: 44818,
    };
  if (!plcConfig.address_access || !plcConfig.address_access.read)
    plcConfig.address_access = { read: [] };
  if (!plcConfig.address_of_value) plcConfig.address_of_value = [""];

  const isSlc = plcConfig.cred.driver === "slc";

  const renderReadRow = (item, rowIndex) => `
            <tr data-row-index="${rowIndex}">
                <td><input type="text" data-field="content" value="${
                  item.content || ""
                }" placeholder="Tag Name"></td>
                <td><input type="text" data-field="address" value="${
                  item.address || ""
                }" placeholder="PLC Tag"></td>
                <td><input type="checkbox" data-field="read" ${
                  item.read !== false ? "checked" : ""
                }></td>
                <td><input type="checkbox" data-field="write" ${
                  item.write ? "checked" : ""
                }></td>
                <td><button type="button" class="remove-ab-row">-</button></td>
            </tr>
        `;

  const renderAddressValueChain = (chain) => `
            <div class="address-chain" style="display: flex; align-items: center; flex-wrap: wrap; gap: 8px; padding: 16px; border: 2px solid #e0e0e0; border-radius: 8px; background: #f9f9f9;">
                ${chain
                  .map(
                    (value, nodeIndex) => `
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div style="display: flex; flex-direction: column; align-items: center; gap: 4px;">
                            <input type="text" 
                                  data-node-index="${nodeIndex}" 
                                  value="${value || ""}" 
                                  placeholder="Address ${nodeIndex + 1}" 
                                  style="padding: 8px; border: 2px solid #ddd; border-radius: 4px; font-family: monospace; width: 120px;">
                            <button type="button" 
                                    class="remove-node" 
                                    data-node-index="${nodeIndex}"
                                    style="background: #f44336; color: white; border: none; border-radius: 50%; width: 20px; height: 20px; cursor: pointer; font-size: 12px; display: ${
                                      chain.length > 1 ? "block" : "none"
                                    };">×</button>
                        </div>
                        ${
                          nodeIndex < chain.length - 1
                            ? '<div style="color: #4CAF50; font-weight: bold; font-size: 18px; margin: 0 4px;">→</div>'
                            : ""
                        }
                    </div>
                `
                  )
                  .join("")}
                <button type="button" class="add-node" 
                        style="background: #4CAF50; color: white; border: none; border-radius: 50%; width: 32px; height: 32px; cursor: pointer; font-size: 18px; margin-left: 8px;">+</button>
            </div>
        `;

  return `
            <form id="ab-form-${index}">
                <fieldset><legend>Credentials</legend>
                    <label>IP: <input type="text" data-cred-field="ip" value="${
                      plcConfig.cred.ip
                    }"></label>
                    <label>Slot: <input type="number" data-cred-field="slot" value="${
                      plcConfig.cred.slot
                    }"></label>
                    <label>Port: <input type="number" data-cred-field="port" value="${
                      plcConfig.cred.port
                    }"></label>
                </fieldset>
                
                <fieldset><legend>Read/Write Operations</legend>
                    <table>
                        <thead><tr><th>Content</th><th>Address</th><th>Read</th><th>Write</th><th>Remove</th></tr></thead>
                        <tbody class="ab-read-tbody">${plcConfig.address_access.read
                          .map(renderReadRow)
                          .join("")}</tbody>
                    </table>
                    <button type="button" class="add-ab-read-row">+ Add Item</button>
                </fieldset>

                <fieldset id="address-of-value-section-${index}" style="margin-top: 20px; ${
    isSlc ? "display: none;" : ""
  }"><legend>Address of Value</legend>
                    <div style="margin-bottom: 12px; padding: 8px; background: #e3f2fd; border-radius: 4px; font-size: 14px; color: #1565c0;">
                        <strong>Address Chain:</strong> Sequential chain of address values. Use + to add nodes, × to remove specific nodes.
                    </div>
                    <div class="address-value-chain">
                        ${renderAddressValueChain(plcConfig.address_of_value)}
                    </div>
                </fieldset>
            </form>
        `;
}

function bindEventsForPlcEntry(index) {
  const entryElement = document.querySelector(
    `.plc-entry[data-index='${index}']`
  );
  if (!entryElement) return;

  entryElement.querySelector(".plc-header").addEventListener("click", (e) => {
    if (e.target.classList.contains("toggle-plc-details")) {
      // Toggle expanded state
      config.plc_configurations[index].isExpanded =
        !config.plc_configurations[index].isExpanded;
      renderModbusTcpPanel();
    }
  });

  entryElement
    .querySelector(".plc-type-select")
    .addEventListener("change", (e) => {
      config.plc_configurations[index].plcType = e.target.value;
      config.plc_configurations[index].PLC = {};
      renderModbusTcpPanel();
    });

  // Driver type selection for Allen Bradley - with conditional visibility
  const driverSelect = entryElement.querySelector(".plc-driver-select");
  if (driverSelect) {
    driverSelect.addEventListener("change", (e) => {
      const plcConfig = config.plc_configurations[index].PLC;
      if (!plcConfig.cred) plcConfig.cred = {};
      plcConfig.cred.driver = e.target.value;

      // Toggle Address of Value section visibility based on driver type
      const addressSection = document.getElementById(
        `address-of-value-section-${index}`
      );
      if (addressSection) {
        if (e.target.value === "slc") {
          addressSection.style.display = "none";
        } else {
          addressSection.style.display = "block";
        }
      }
    });
  }

  const nameInput = entryElement.querySelector(".plc-name-input");
  if (nameInput) {
    nameInput.addEventListener("input", (e) => {
      const plcConfig = config.plc_configurations[index].PLC;
      if (!plcConfig.Database) plcConfig.Database = {};
      plcConfig.Database.table_name = e.target.value;
    });
  }

  entryElement
    .querySelector(".remove-plc-entry")
    .addEventListener("click", () => {
      config.plc_configurations.splice(index, 1);
      renderModbusTcpPanel();
    });
  console.log(config.plc_configurations);

  const plcType = config.plc_configurations[index].plcType;
  if (plcType === "Siemens") bindSiemensFormEvents(index);
  else if (plcType === "Allen Bradley") bindAllenBradleyFormEvents(index);
}

function bindAllenBradleyFormEvents(index) {
  const form = document.getElementById(`ab-form-${index}`);
  if (!form) return;

  form.addEventListener("input", (e) => {
    const plcConfig = config.plc_configurations[index].PLC;

    if (e.target.dataset.credField) {
      plcConfig.cred[e.target.dataset.credField] = e.target.value;
    } else if (e.target.dataset.nodeIndex !== undefined) {
      // Handle Address of Value chain inputs
      const nodeIndex = parseInt(e.target.dataset.nodeIndex, 10);
      const value = e.target.value;

      if (!plcConfig.address_of_value) {
        plcConfig.address_of_value = [];
      }
      plcConfig.address_of_value[nodeIndex] = value;
    } else {
      // Handle read/write table inputs
      const row = e.target.closest("tr");
      if (row) {
        const rowIndex = parseInt(row.dataset.rowIndex, 10);
        const field = e.target.dataset.field;
        const value =
          e.target.type === "checkbox" ? e.target.checked : e.target.value;
        plcConfig.address_access.read[rowIndex][field] = value;
      }
    }
  });

  form.querySelector(".add-ab-read-row").addEventListener("click", () => {
    const plcConfig = config.plc_configurations[index].PLC;
    if (!plcConfig.address_access.read) plcConfig.address_access.read = [];
    plcConfig.address_access.read.push({ read: true });
    renderModbusTcpPanel();
  });

  form.querySelector(".ab-read-tbody").addEventListener("click", (e) => {
    if (e.target.classList.contains("remove-ab-row")) {
      const rowIndex = parseInt(e.target.closest("tr").dataset.rowIndex, 10);
      config.plc_configurations[index].PLC.address_access.read.splice(
        rowIndex,
        1
      );
      renderModbusTcpPanel();
    }
  });

  // Handle address chain events - only if the section exists and is visible
  const addressChainContainer = form.querySelector(".address-value-chain");
  if (addressChainContainer) {
    addressChainContainer.addEventListener("click", (e) => {
      const plcConfig = config.plc_configurations[index].PLC;

      if (e.target.classList.contains("add-node")) {
        // Add new node to the chain
        if (!plcConfig.address_of_value) plcConfig.address_of_value = [];
        plcConfig.address_of_value.push("");
        renderModbusTcpPanel();
      } else if (e.target.classList.contains("remove-node")) {
        // Remove specific node from the chain
        const nodeIndex = parseInt(e.target.dataset.nodeIndex, 10);
        if (plcConfig.address_of_value.length > 1) {
          // Keep at least one node
          plcConfig.address_of_value.splice(nodeIndex, 1);
          renderModbusTcpPanel();
        }
      }
    });
  }
}

// ===================== Minimal palette =====================
const UI_ACCENT = "#2563eb";
const UI_TEXT = "#111827";
const UI_MUTED = "#6b7280";
const UI_BORDER = "#e5e7eb";
const UI_TAB_ACTIVE = "#4338ca";
const UI_BG_MILD = "#f9fafb";
const UI_TAB_ACTIVE_BG = "#eef2ff";

function tabBtnStyle(active) {
  const base = `border:1px solid ${UI_BORDER}; background:#fff; color:${UI_TEXT}; padding:6px 10px; margin:6px 8px 0 0; border-radius:8px; cursor:pointer; font-size:13px;`;
  const act = `border-color:${UI_TAB_ACTIVE}; color:${UI_TAB_ACTIVE}; background:${UI_TAB_ACTIVE_BG};`;
  return base + (active ? act : "");
}
function btnPrimaryStyle() {
  return `background:${UI_ACCENT}; color:#fff; border:1px solid ${UI_ACCENT}; padding:6px 12px; border-radius:8px; font-size:13px; cursor:pointer;`;
}
function btnDarkStyle() {
  return `background:${UI_TEXT}; color:#fff; border:1px solid ${UI_TEXT}; padding:6px 12px; border-radius:8px; font-size:13px; cursor:pointer;`;
}
function inputStyle(w = "") {
  const wcss = w ? `width:${w};` : "";
  return `${wcss} padding:6px 8px; border:1px solid ${UI_BORDER}; border-radius:6px; font-size:13px; color:${UI_TEXT}; background:#fff;`;
}
const thStyle = `padding:8px 10px; font-weight:600; color:${UI_TEXT}; background:${UI_BG_MILD}; border-bottom:1px solid ${UI_BORDER}; text-align:left;`;
const tdStyle = `padding:8px 10px; color:${UI_TEXT}; border-bottom:1px solid #f3f4f6;`;

// ===================== Legacy -> nested migration for Modbus =====================
function migrateModbusAlertsToNested() {
  if (!window.config) window.config = {};
  if (!config.alarmSettings) config.alarmSettings = {};
  if (!config.alarmSettings.Modbus) config.alarmSettings.Modbus = {};
  const mod = config.alarmSettings.Modbus;
  if (!Array.isArray(mod.alerts)) return;

  const next = {};
  for (const a of mod.alerts || []) {
    const brand = a.brand_key == null ? "" : String(a.brand_key).trim();
    const sid = a.slave_id == null ? "" : String(a.slave_id).trim();
    if (!brand || !sid) continue;
    if (!next[brand]) next[brand] = { slaves: {} };
    if (!next[brand].slaves[sid]) next[brand].slaves[sid] = { alerts: [] };
    next[brand].slaves[sid].alerts.push({
      condition: a.condition,
      threshold: a.threshold,
      contact: a.contact,
      message: a.message,
      enabled: !!a.enabled,
    });
  }
  delete mod.alerts;
  Object.assign(mod, next);
  if (typeof saveConfig === "function") {
    try {
      saveConfig();
    } catch (e) {}
  }
}

// ===================== Data accessors: Energy Meter =====================
function getEnergyBrands() {
  const em =
    config.ModbusRTU && config.ModbusRTU.energyMeter
      ? config.ModbusRTU.energyMeter
      : null;
  const brands = em && em.brands ? em.brands : {};
  const order =
    em && Array.isArray(em.order) && em.order.length
      ? em.order
      : Object.keys(brands);
  return { brands, order };
}
function getEnergyBrandSlaves(brandKey) {
  const { brands } = getEnergyBrands();
  const b = brands[brandKey];
  const slaves = b && Array.isArray(b.slaves) ? b.slaves : [];
  return slaves
    .map((s) => (s && s.id != null ? String(s.id) : ""))
    .filter(Boolean);
}
function getModbusAlerts(brandKey, slaveId, registerName) {
  const em = config.alarmSettings?.energyMeter;
  if (!em) return [];

  const brand = em[brandKey];
  if (!brand) return [];

  const slave = brand[String(slaveId)];
  if (!slave) return [];

  const alerts = slave[registerName];
  return Array.isArray(alerts) ? alerts : [];
}
function ensureModbusAlerts(brandKey, slaveId, registerName) {
  if (!config.alarmSettings) config.alarmSettings = {};
  if (!config.alarmSettings.energyMeter) config.alarmSettings.energyMeter = {};

  const em = config.alarmSettings.energyMeter;

  if (!em[brandKey]) em[brandKey] = {};
  if (!em[brandKey][String(slaveId)]) em[brandKey][String(slaveId)] = {};
  if (!Array.isArray(em[brandKey][String(slaveId)][registerName]))
    em[brandKey][String(slaveId)][registerName] = [];

  return em[brandKey][String(slaveId)][registerName];
}

function setModbusAlerts(brandKey, slaveId, registerName, rows) {
  if (!Array.isArray(rows)) {
    console.error("setModbusAlerts: rows is not array", rows);
    return;
  }

  const list = ensureModbusAlerts(brandKey, slaveId, registerName);

  list.length = 0;
  list.push(
    ...rows.map((r) => ({
      condition: r.condition || "<=",
      threshold: r.threshold === "" ? "" : Number(r.threshold),
      contact: r.contact || "",
      email: r.email || "",
      message: r.message || `${brandKey} S${slaveId} ${registerName}`,
      enabled: !!r.enabled,
    }))
  );

  try {
    saveConfig?.();
  } catch (e) {
    console.error("saveConfig failed", e);
  }
}

function pickFirstEnergyBrandAndSlave() {
  const { brands, order } = getEnergyBrands();
  const brandKey =
    order && order.length ? order[0] : Object.keys(brands)[0] || "";
  const slave = brandKey ? getEnergyBrandSlaves(brandKey)[0] || "" : "";
  return { brandKey, slaveId: slave };
}

// ===================== Data accessors: PLC =====================
function getPlcBrandsAndSlaves() {
  const list =
    config.ModbusRTU && Array.isArray(config.ModbusRTU.PLC)
      ? config.ModbusRTU.PLC
      : [];
  const map = {};
  list.forEach((entry) => {
    const plcType = entry && entry.plcType ? String(entry.plcType) : "Unknown";
    const cred = entry && entry.PLC && entry.PLC.cred ? entry.PLC.cred : {};
    const sid = cred && cred.slave_id != null ? String(cred.slave_id) : "";
    if (!map[plcType]) map[plcType] = new Set();
    if (sid) map[plcType].add(sid);
  });
  const out = {};
  Object.keys(map).forEach((k) => (out[k] = Array.from(map[k]).sort()));
  return out;
}
function ensurePlcNested() {
  if (!window.config) window.config = {};
  if (!config.alarmSettings) config.alarmSettings = {};
  if (!config.alarmSettings.Modbus) config.alarmSettings.Modbus = {};
  if (!config.alarmSettings.PLC) config.alarmSettings.PLC = {};
  return config.alarmSettings.PLC;
}
function getPlcAlerts(brandKey, slaveId) {
  const plc = ensurePlcNested();
  const brand = plc[brandKey] || {};
  const slaves = brand.slaves || {};
  const node = slaves[slaveId] || {};
  return Array.isArray(node.alerts) ? node.alerts : [];
}
function ensurePlcAlerts(brandKey, slaveId) {
  const plc = ensurePlcNested();
  if (!plc[brandKey]) plc[brandKey] = { slaves: {} };
  if (!plc[brandKey].slaves[slaveId])
    plc[brandKey].slaves[slaveId] = { alerts: [] };
  return plc[brandKey].slaves[slaveId].alerts;
}
function setPlcAlerts(brandKey, slaveId, rows) {
  const plc = ensurePlcNested();
  ensurePlcAlerts(brandKey, slaveId);
  plc[brandKey].slaves[slaveId].alerts = rows.map((r) => ({
    condition: r.condition || "<=",
    threshold: r.threshold === "" ? "" : Number(r.threshold),
    contact: r.contact || "",
    message: r.message || `${brandKey} S${slaveId}`,
    enabled: !!r.enabled,
  }));
  if (typeof saveConfig === "function") {
    try {
      saveConfig();
    } catch (e) {}
  }
}
function pickFirstPlcBrandAndSlave() {
  const map = getPlcBrandsAndSlaves();
  const brands = Object.keys(map);
  const brand = brands[0] || "Allen Bradley";
  const slave = map[brand] && map[brand][0] ? map[brand][0] : "";
  return { brandKey: brand, slaveId: slave };
}

// ===================== Existing Digital & Analog forms are kept as-is =====================
// renderDigitalIOAlertsForm(subTab)
// renderAnalogAlertsForm(subTab)
// setupTabHandlers handles Digital/Analog paths inside

// ===================== Modbus composite UI: Energy + PLC =====================
function renderAlarmSettings(mainTab = "Modbus", subTab = "Channel 1") {
  migrateModbusAlertsToNested();

  // const MainTabs = ["Digital I/O", "Modbus", "Analog", "SMS Settings"];
  const MainTabs = ["Modbus", "Analog"];
  const mainTabsHtml = MainTabs.map(
    (t) =>
      `<button class="main-tab-btn${
        t === mainTab ? " active" : ""
      }" data-tab="${t}" style="${tabBtnStyle(t === mainTab)}"
            onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
            onmouseleave="if(!this.classList.contains('active')) this.style.background='${UI_TAB_ACTIVE_BG}'"
          >${t}</button>`
  ).join("");

  const panel = document.getElementById("main-panel");
  panel.innerHTML = `
        <div style="font-size:16px; font-weight:600; color:${UI_TEXT}; margin:4px 0 8px;">Alarms</div>
        <div class="tabs-line" style="display:flex; flex-wrap:wrap; align-items:center;">${mainTabsHtml}</div>
        <div class="panel-content" style="margin-top: 12px;">
          ${renderAlarmSettingsContent(mainTab, subTab)}
        </div>
      `;

  // Ensure Modbus subsections are initialized after containers exist
  if (mainTab === "Modbus") {
    // Call directly (containers exist now)
    try {
      renderEnergyMeterSection();
    } catch (e) {
      console.error(e);
    }
    // try {
    //   renderPlcSection();
    // } catch (e) {
    //   console.error(e);
    // }

    // If preferring next-frame timing, use:
    // requestAnimationFrame(() => {
    //   try { renderEnergyMeterSection(); } catch(e) { console.error(e); }
    //   try { renderPlcSection(); } catch(e) { console.error(e); }
    // });
  }

  document.querySelectorAll(".main-tab-btn").forEach(
    (btn) =>
      (btn.onclick = () => {
        const newMainTab = btn.dataset.tab;
        let defaultSub = subTab;
        if (newMainTab === "Digital I/O") defaultSub = "Channel 1";
        if (newMainTab === "Analog") defaultSub = "A1";
        if (newMainTab === "Modbus") defaultSub = "Energy Meter";
        renderAlarmSettings(newMainTab, defaultSub);
      })
  );

  setupTabHandlers(mainTab, subTab);
}

function renderAlarmSettingsContent(mainTab, subTab) {
  if (mainTab === "Digital I/O") {
    return renderDigitalIOAlertsForm(subTab);
  }
  if (mainTab === "Analog") {
    return renderAnalogAlertsForm(subTab);
  }
  if (mainTab === "SMS Settings") {
    return `<div style="padding: 20px; color: #666;">SMS Settings UI coming soon</div>`;
  }
  if (mainTab === "Modbus") {
    return `
          <div style="margin:14px 0 10px; color:${UI_TEXT}; font-weight:600;"></div>
          <div id="em-section"></div>
          <div style="margin:18px 0 10px; color:${UI_TEXT}; font-weight:600;"></div>
          <div id="plc-section"></div>
        `;
  }
  return `<div style="padding: 20px;">Please select a tab</div>`;
}

function renderDigitalIOAlertsForm(channel) {
  // Ensure store exists
  config.alarmSettings = config.alarmSettings || {};
  config.alarmSettings["Digital I/O"] =
    config.alarmSettings["Digital I/O"] || {};
  config.alarmSettings["Digital I/O"][channel] = config.alarmSettings[
    "Digital I/O"
  ][channel] || { alerts: [] };

  const channelStore = config.alarmSettings["Digital I/O"][channel];
  channelStore.alerts = Array.isArray(channelStore.alerts)
    ? channelStore.alerts
    : [];

  const rows = channelStore.alerts
    .map(
      (row, i) => `
          <tr>
            <td>${i + 1}</td>
            <td>
              <select name="trigger_${i}">
                <option ${
                  row.trigger === "High" ? "selected" : ""
                }>High</option>
                <option ${row.trigger === "Low" ? "selected" : ""}>Low</option>
                <option ${
                  row.trigger === "Rising Edge" ? "selected" : ""
                }>Rising Edge</option>
                <option ${
                  row.trigger === "Falling Edge" ? "selected" : ""
                }>Falling Edge</option>
              </select>
            </td>
            <td><input type="text" name="contact_${i}" value="${
        row.contact ?? ""
      }" style="width:140px"></td>
            <td><input type="text" name="message_${i}" value="${
        row.message ?? ""
      }" style="width:160px"></td>
            <td style="text-align:center;"><input type="checkbox" name="enabled_${i}" ${
        row.enabled ? "checked" : ""
      }></td>
            <td style="text-align:center;"><button type="button" class="del-alert" data-index="${i}" title="Remove" style="color:#c00;background:transparent;border:none;font-size:18px;cursor:pointer;">×</button></td>
          </tr>
        `
    )
    .join("");

  return `
        <form id="digital-io-alerts-form">
          <div style="margin-bottom:10px;">
            <button type="button" class="button-primary" id="add-digital-alert-btn">+ Add Alert</button>
          </div>
          <div style="overflow-x:auto;">
            <table class="channel-table" style="width:100%; background:#fff;">
              <tr>
                <th>#</th>
                <th>Trigger</th>
                <th>Contact</th>
                <th>Message</th>
                <th>Enabled</th>
                <th>Delete</th>
              </tr>
              ${rows}
            </table>
          </div>
          <div style="margin-top:12px;">
            <button type="submit" class="button-primary">Save</button>
            <span class="success-tick" id="digital-io-alerts-tick" style="display:none;">✔</span>
          </div>
        </form>
      `;
}

function renderAnalogAlertsForm(channel) {
  // Ensure store exists
  config.alarmSettings = config.alarmSettings || {};
  config.alarmSettings.Analog = config.alarmSettings.Analog || {};
  config.alarmSettings.Analog[channel] = config.alarmSettings.Analog[
    channel
  ] || { alerts: [] };

  const channelStore = config.alarmSettings.Analog[channel];
  channelStore.alerts = Array.isArray(channelStore.alerts)
    ? channelStore.alerts
    : [];

  const rows = channelStore.alerts
    .map(
      (row, i) => `
          <tr>
            <td>${i + 1}</td>
            <td>
              <select name="condition_${i}">
                <option ${row.condition === "<=" ? "selected" : ""}><=</option>
                <option ${row.condition === "<" ? "selected" : ""}><</option>
                <option ${row.condition === ">=" ? "selected" : ""}>>=</option>
                <option ${row.condition === ">" ? "selected" : ""}>></option>
              </select>
            </td>
            <td><input type="number" step="any" name="threshold_${i}" value="${
        row.threshold ?? ""
      }" style="width:80px"></td>
            <td><input type="number" name="delay_${i}" value="${
        row.delay ?? ""
      }" placeholder="seconds" style="width:80px"></td>
            <td><input type="email" name="email_${i}" value="${
        row.email ?? ""
      }" placeholder="email@example.com" style="width:140px"></td>
            <td><input type="text" name="contact_${i}" value="${
        row.contact ?? ""
      }" style="width:140px"></td>
            <td><input type="text" name="message_${i}" value="${
        row.message ?? ""
      }" style="width:160px"></td>
            <td style="text-align:center;"><input type="checkbox" name="enabled_${i}" ${
        row.enabled ? "checked" : ""
      }></td>
            <td style="text-align:center;"><button type="button" class="del-alert" data-index="${i}" title="Remove" style="color:#c00;background:transparent;border:none;font-size:18px;cursor:pointer;">×</button></td>
          </tr>
        `
    )
    .join("");

  return `
        <form id="analog-alerts-form">
          <div style="margin-bottom:10px;">
            <button type="button" class="button-primary" id="add-analog-alert-btn">+ Add Alert</button>
          </div>
          <div style="overflow-x:auto;">
            <table class="channel-table" style="width:100%; background:#fff;">
              <tr>
                <th>#</th>
                <th>Condition</th>
                <th>Threshold</th>
                <th>Delay (s)</th>
                <th>Email</th>
                <th>Contact</th>
                <th>Message</th>
                <th>Enabled</th>
                <th>Delete</th>
              </tr>
              ${rows}
            </table>
          </div>
          <div style="margin-top:12px;">
            <button type="submit" class="button-primary">Save</button>
            <span class="success-tick" id="analog-alerts-tick" style="display:none;">✔</span>
          </div>
        </form>
      `;
}

// ===================== Energy Meter render + handlers =====================
// Updated helper functions for Energy Meter registers and alerts
function getEnergyBrandRegisters(brandKey, slaveId) {
  const { brands } = getEnergyBrands();
  return brands[brandKey]?.registersBySlave?.[slaveId] || [];
}

function getEnergyBrandSlaves(brandKey) {
  const { brands } = getEnergyBrands();
  return brands[brandKey]?.slaves?.map((s) => s.id.toString()) || [];
}

// ===================== Updated Energy Meter with Register Tabs =====================
function renderEnergyMeterSection(
  selectedBrand = "",
  selectedSlave = "",
  selectedRegister = ""
) {
  const mount = document.getElementById("em-section");
  if (!mount) return;

  const { brands, order } = getEnergyBrands();
  if (!selectedBrand || !selectedSlave || !selectedRegister) {
    const pick = pickFirstEnergyBrandAndSlave();
    selectedBrand = selectedBrand || pick.brandKey;
    selectedSlave = selectedSlave || pick.slaveId;
    const firstReg = getEnergyBrandRegisters(selectedBrand, selectedSlave)[0];
    selectedRegister = selectedRegister || (firstReg ? firstReg.name : "");
  }

  const brandTabs = (order.length ? order : Object.keys(brands))
    .filter((k) => brands[k])
    .map((bk) => {
      const label = brands[bk]?.label || bk;
      const active = bk === selectedBrand;
      return `<button class="em-brand" data-brand="${bk}" style="${tabBtnStyle(
        active
      )}"
          onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
          onmouseleave="if(!this.classList.contains('active')) this.style.background='#fff'"
        >${label}</button>`;
    })
    .join("");

  const slaveTabs = getEnergyBrandSlaves(selectedBrand)
    .map((sid) => {
      const active = sid === selectedSlave;
      return `<button class="em-slave" data-slave="${sid}" style="${tabBtnStyle(
        active
      )}"
          onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
          onmouseleave="if(!this.classList.contains('active')) this.style.background='#fff'"
        >Slave ${sid}</button>`;
    })
    .join("");

  // SHOW ALL REGISTERS - removed .filter(r => r.enabled)
  const registerTabs = getEnergyBrandRegisters(selectedBrand, selectedSlave)
    .map((reg) => {
      const active = reg.name === selectedRegister;
      return `<button class="em-register" data-register="${
        reg.name
      }" style="${tabBtnStyle(active)}"
          onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
          onmouseleave="if(!this.classList.contains('active')) this.style.background='#fff'"
        >${reg.name}</button>`;
    })
    .join("");

  const registerRow =
    registerTabs ||
    '<span style="color:${UI_MUTED}; font-size:14px;">No registers available</span>';

  mount.innerHTML = `
      <div style="display:flex; flex-wrap:wrap; align-items:center;">${brandTabs}</div>
      <div style="display:flex; flex-wrap:wrap; align-items:center; margin-top:6px;">${slaveTabs}</div>
      <div style="display:flex; flex-wrap:wrap; align-items:center; margin-top:6px;">
        ${registerRow}
      </div>
      <div style="margin-top:10px; background:#fff; border:1px solid ${UI_BORDER}; border-radius:10px; padding:12px;">
        ${renderEnergyAlertsTable(
          selectedBrand,
          selectedSlave,
          selectedRegister
        )}
      </div>
    `;

  // Rest of the event handlers remain the same...
  mount.querySelectorAll(".em-brand").forEach((btn) => {
    btn.onclick = () => {
      const newBrand = btn.dataset.brand;
      const firstSlave = getEnergyBrandSlaves(newBrand)[0] || "";
      const firstReg = getEnergyBrandRegisters(newBrand, firstSlave)[0];
      renderEnergyMeterSection(
        newBrand,
        firstSlave,
        firstReg ? firstReg.name : ""
      );
    };
  });

  mount.querySelectorAll(".em-slave").forEach((btn) => {
    btn.onclick = () => {
      const newSlave = btn.dataset.slave;
      const firstReg = getEnergyBrandRegisters(selectedBrand, newSlave)[0];
      renderEnergyMeterSection(
        selectedBrand,
        newSlave,
        firstReg ? firstReg.name : ""
      );
    };
  });

  mount.querySelectorAll(".em-register").forEach((btn) => {
    btn.onclick = () =>
      renderEnergyMeterSection(
        selectedBrand,
        selectedSlave,
        btn.dataset.register
      );
  });

  setupEnergyHandlers(selectedBrand, selectedSlave, selectedRegister);
}

function getRegisterProcessRange(brandKey, slaveId, registerName) {
  const regs = getEnergyBrandRegisters(brandKey, slaveId);
  const reg = regs.find((r) => r.name === registerName);
  if (!reg) return { min: null, max: null };

  const min = reg.process_min !== "" ? Number(reg.process_min) : null;
  const max = reg.process_max !== "" ? Number(reg.process_max) : null;
  return { min, max };
}

function renderEnergyAlertsTable(brandKey, slaveId, registerName) {
  const alerts = (() => {
    const a = getModbusAlerts(brandKey, slaveId, registerName);
    return Array.isArray(a) ? a : [];
  })();

  const rows = alerts
    .map(
      (row, i) => `
      <tr data-row="${i}">
        <td style="${tdStyle} color:${UI_MUTED};">${i + 1}</td>
        <td style="${tdStyle}">${brandKey}</td>
        <td style="${tdStyle}">${slaveId}</td>
        <td style="${tdStyle}">${registerName}</td>
        <td style="${tdStyle}">
          <select name="condition_${i}" style="${inputStyle()}">
            <option ${row.condition === "<=" ? "selected" : ""}>&lt;=</option>
            <option ${row.condition === "<" ? "selected" : ""}>&lt;</option>
            <option ${row.condition === ">=" ? "selected" : ""}>&gt;=</option>
            <option ${row.condition === ">" ? "selected" : ""}>&gt;</option>
          </select>
        </td>
        <td style="${tdStyle}"><input type="number" step="any" name="threshold_${i}" value="${
        row.threshold ?? ""
      }" style="${inputStyle("100px")}"></td>
        <td style="${tdStyle}"><input type="email" name="email_${i}" value="${
        row.email ?? ""
      }" placeholder="email@example.com" style="${inputStyle("120px")}"></td>
        <td style="${tdStyle}"><input type="text" name="contact_${i}" value="${
        row.contact ?? ""
      }" style="${inputStyle("140px")}"></td>
        <td style="${tdStyle}"><input type="text" name="message_${i}" value="${
        row.message ?? ""
      }" style="${inputStyle("160px")}"></td>
        <td style="${tdStyle}; text-align:center;"><input type="checkbox" name="enabled_${i}" ${
        row.enabled ? "checked" : ""
      }></td>
        <td style="${tdStyle}; text-align:center;">
          <button type="button" class="em-del" data-idx="${i}" style="border:1px solid #ef4444; color:#ef4444; background:#fff; padding:4px 8px; border-radius:4px; font-size:11px; cursor:pointer;">Remove</button>
        </td>
      </tr>
    `
    )
    .join("");

  return `
      <form id="em-form" onsubmit="return false;" data-brand="${brandKey}" data-slave="${slaveId}" data-register="${registerName}">
        <div style="display:flex; align-items:center; justify-content:space-between; gap:8px; flex-wrap:wrap; margin-bottom:10px;">
          <div style="color:${UI_MUTED}; font-size:13px;">
            Brand <span style="color:${UI_TEXT}; font-weight:600;">${brandKey}</span> · 
            Slave <span style="color:${UI_TEXT}; font-weight:600;">${slaveId}</span> · 
            Register <span style="color:${UI_TEXT}; font-weight:600;">${registerName}</span>
          </div>
          <div style="display:flex; gap:8px;">
            <button type="button" id="em-add" style="${btnPrimaryStyle()}">+ Add Alert</button>
            <button type="submit" id="em-save" style="${btnDarkStyle()}">Save</button>
            <span id="em-tick" style="display:none; color:${UI_ACCENT}; font-weight:600; align-self:center;">Saved ✔</span>
          </div>
        </div>
        <div style="overflow:auto; border:1px solid ${UI_BORDER}; border-radius:8px;">
          <table style="width:100%; border-collapse:separate; border-spacing:0;">
            <thead>
              <tr>
                <th style="${thStyle}">#</th>
                <th style="${thStyle}">Brand</th>
                <th style="${thStyle}">Slave ID</th>
                <th style="${thStyle}">Register</th>
                <th style="${thStyle}">Condition</th>
                <th style="${thStyle}">Threshold</th>
                <th style="${thStyle}">Email</th>
                <th style="${thStyle}">Contact</th>
                <th style="${thStyle}">Message</th>
                <th style="${thStyle}; text-align:center;">Enabled</th>
                <th style="${thStyle}; text-align:center;">Delete</th>
              </tr>
            </thead>
            <tbody>
              ${
                rows ||
                `<tr><td colspan="11" style="padding:20px; color:${UI_MUTED}; text-align:center;">
                No alerts for ${brandKey} · Slave ${slaveId} · ${registerName}. Click "+ Add Alert".
              </td></tr>`
              }
            </tbody>
          </table>
        </div>
      </form>
    `;
}

function setupEnergyHandlers(brandKey, slaveId, registerName) {
  const form = document.getElementById("em-form");
  if (!form) return;

  // Add/Delete handlers
  form.addEventListener("click", (e) => {
    const add = e.target.closest("#em-add");
    if (add) {
      const list = ensureModbusAlerts(brandKey, slaveId, registerName);
      list.push({
        condition: "<=",
        threshold: "",
        email: "",
        contact: "",
        message: `${brandKey} S${slaveId} ${registerName}`,
        enabled: true,
      });
      if (typeof saveConfig === "function") saveConfig();
      renderEnergyMeterSection(brandKey, slaveId, registerName);
      return;
    }

    const del = e.target.closest(".em-del");
    if (del) {
      const idx = Number(del.dataset.idx);
      const list = ensureModbusAlerts(brandKey, slaveId, registerName);
      if (idx >= 0 && idx < list.length) {
        list.splice(idx, 1);
        if (typeof saveConfig === "function") saveConfig();
        renderEnergyMeterSection(brandKey, slaveId, registerName);
      }
    }
  });

  // Save handler
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    try {
      const { min: processMin, max: processMax } = getRegisterProcessRange(
        brandKey,
        slaveId,
        registerName
      );

      const trs = Array.from(form.querySelectorAll("tbody tr"));

      const rows = trs.map((tr, i) => {
        const condition =
          tr.querySelector(`select[name="condition_${i}"]`)?.value || "<=";

        const thresholdRaw = tr.querySelector(
          `input[name="threshold_${i}"]`
        )?.value;

        const threshold = thresholdRaw === "" ? null : Number(thresholdRaw);

        // ---- VALIDATION ----
        if (threshold !== null) {
          if (
            (condition === "<" || condition === "<=") &&
            processMin !== null
          ) {
            if (threshold < processMin) {
              throw new Error(
                `Row ${
                  i + 1
                }: Threshold (${threshold}) cannot be less than process minimum (${processMin})`
              );
            }
          }

          if (
            (condition === ">" || condition === ">=") &&
            processMax !== null
          ) {
            if (threshold > processMax) {
              throw new Error(
                `Row ${
                  i + 1
                }: Threshold (${threshold}) cannot exceed process maximum (${processMax})`
              );
            }
          }
        }

        return {
          condition,
          threshold: thresholdRaw || "",
          email: tr.querySelector(`input[name="email_${i}"]`)?.value || "",
          contact: tr.querySelector(`input[name="contact_${i}"]`)?.value || "",
          message:
            tr.querySelector(`input[name="message_${i}"]`)?.value ||
            `${brandKey} S${slaveId} ${registerName}`,
          enabled:
            tr.querySelector(`input[name="enabled_${i}"]`)?.checked || false,
        };
      });

      setModbusAlerts(brandKey, slaveId, registerName, rows);

      const tick = document.getElementById("em-tick");
      if (tick) {
        tick.style.display = "inline-block";
        setTimeout(() => (tick.style.display = "none"), 1100);
      }

      if (typeof saveConfig === "function") saveConfig();
    } catch (ex) {
      alert(ex.message); // ← IMPORTANT: show user exactly what’s wrong
      console.error("[EM] Save failed", ex);
    }
  });
}

// ===================== PLC render + handlers =====================
function renderPlcSection(selectedBrand = "", selectedSlave = "") {
  const mount = document.getElementById("plc-section");
  if (!mount) return;

  const map = getPlcBrandsAndSlaves();
  const brands = Object.keys(map);
  if (!selectedBrand || !selectedSlave) {
    const pick = pickFirstPlcBrandAndSlave();
    selectedBrand = selectedBrand || pick.brandKey;
    selectedSlave = selectedSlave || pick.slaveId;
  }

  const brandTabs = brands
    .map((bk) => {
      const active = bk === selectedBrand;
      return `<button class="plc-brand" data-brand="${bk}" style="${tabBtnStyle(
        active
      )}"
          onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
          onmouseleave="if(!this.classList.contains('active')) this.style.background='#fff'"
        >${bk}</button>`;
    })
    .join("");

  const slaveTabs = (map[selectedBrand] || [])
    .map((sid) => {
      const active = sid === selectedSlave;
      return `<button class="plc-slave" data-slave="${sid}" style="${tabBtnStyle(
        active
      )}"
          onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
          onmouseleave="if(!this.classList.contains('active')) this.style.background='#fff'"
        >Slave ${sid}</button>`;
    })
    .join("");

  mount.innerHTML = `
        <div style="display:flex; flex-wrap:wrap; align-items:center;">${brandTabs}</div>
        <div style="display:flex; flex-wrap:wrap; align-items:center; margin-top:6px;">${slaveTabs}</div>
        <div style="margin-top:10px; background:#fff; border:1px solid ${UI_BORDER}; border-radius:10px; padding:12px;">
          ${renderPlcAlertsTable(selectedBrand, selectedSlave)}
        </div>
      `;

  mount.querySelectorAll(".plc-brand").forEach((btn) => {
    btn.onclick = () => {
      const newBrand = btn.dataset.brand;
      const firstSlave =
        map[newBrand] && map[newBrand][0] ? map[newBrand][0] : "";
      renderPlcSection(newBrand, firstSlave);
    };
  });
  mount.querySelectorAll(".plc-slave").forEach((btn) => {
    btn.onclick = () => renderPlcSection(selectedBrand, btn.dataset.slave);
  });

  setupPlcHandlers(selectedBrand, selectedSlave);
}

function renderPlcAlertsTable(brandKey, slaveId) {
  const alerts = getPlcAlerts(brandKey, slaveId);

  const rows = alerts
    .map(
      (row, i) => `
        <tr data-row="${i}">
          <td style="${tdStyle} color:${UI_MUTED};">${i + 1}</td>
          <td style="${tdStyle}">${brandKey}</td>
          <td style="${tdStyle}">${slaveId}</td>
          <td style="${tdStyle}">
            <select name="condition_${i}" style="${inputStyle()}">
              <option ${row.condition === "<=" ? "selected" : ""}>&lt;=</option>
              <option ${row.condition === "<" ? "selected" : ""}>&lt;</option>
              <option ${row.condition === ">=" ? "selected" : ""}>&gt;=</option>
              <option ${row.condition === ">" ? "selected" : ""}>&gt;</option>
            </select>
          </td>
          <td style="${tdStyle}"><input type="number" step="any" name="threshold_${i}" value="${
        row.threshold ?? ""
      }" style="${inputStyle("120px")}"></td>
          <td style="${tdStyle}"><input type="text" name="contact_${i}" value="${
        row.contact ?? ""
      }" style="${inputStyle("180px")}"></td>
          <td style="${tdStyle}"><input type="text" name="message_${i}" value="${
        row.message ?? ""
      }" style="${inputStyle("220px")}"></td>
          <td style="${tdStyle}; text-align:center;"><input type="checkbox" name="enabled_${i}" ${
        row.enabled ? "checked" : ""
      }></td>
          <td style="${tdStyle}; text-align:center;">
            <button type="button" class="plc-del" data-idx="${i}" style="border:1px solid #ef4444; color:#ef4444; background:#fff; padding:6px 10px; border-radius:6px; font-size:12px; cursor:pointer;">Remove</button>
          </td>
        </tr>
      `
    )
    .join("");

  return `
        <form id="plc-form" onsubmit="return false;" data-brand="${brandKey}" data-slave="${slaveId}">
          <div style="display:flex; align-items:center; justify-content:space-between; gap:8px; flex-wrap:wrap; margin-bottom:10px;">
            <div style="color:${UI_MUTED}; font-size:13px;">Brand <span style="color:${UI_TEXT}; font-weight:600;">${brandKey}</span> · Slave <span style="color:${UI_TEXT}; font-weight:600;">${slaveId}</span></div>
            <div style="display:flex; gap:8px;">
              <button type="button" id="plc-add" style="${btnPrimaryStyle()}">+ Add Alert</button>
              <button type="submit" id="plc-save" style="${btnDarkStyle()}">Save</button>
              <span id="plc-tick" style="display:none; color:${UI_ACCENT}; font-weight:600; align-self:center;">Saved</span>
            </div>
          </div>
          <div style="overflow:auto; border:1px solid ${UI_BORDER}; border-radius:8px;">
            <table style="width:100%; border-collapse:separate; border-spacing:0;">
              <thead>
                <tr>
                  <th style="${thStyle}">#</th>
                  <th style="${thStyle}">Brand</th>
                  <th style="${thStyle}">Slave ID</th>
                  <th style="${thStyle}">Condition</th>
                  <th style="${thStyle}">Threshold</th>
                  <th style="${thStyle}">Contact</th>
                  <th style="${thStyle}">Message</th>
                  <th style="${thStyle}; text-align:center;">Enabled</th>
                  <th style="${thStyle}; text-align:center;">Delete</th>
                </tr>
              </thead>
              <tbody>
                ${
                  rows ||
                  `<tr><td colspan="9" style="padding:14px; color:${UI_MUTED}; text-align:center;">No alerts for <span style="color:${UI_TEXT}; font-weight:600;">${brandKey}</span> · <span style="color:${UI_TEXT}; font-weight:600;">Slave ${slaveId}</span>. Click “Add Alert”.</td></tr>`
                }
              </tbody>
            </table>
          </div>
        </form>
      `;
}

function setupPlcHandlers(brandKey, slaveId) {
  const form = document.getElementById("plc-form");
  if (!form) return;

  form.addEventListener("click", (e) => {
    const add = e.target.closest("#plc-add");
    if (add) {
      const list = ensurePlcAlerts(brandKey, slaveId);
      list.push({
        condition: "<=",
        threshold: "",
        contact: "",
        message: `${brandKey} S${slaveId}`,
        enabled: true,
      });
      if (typeof saveConfig === "function") {
        try {
          saveConfig();
        } catch (e) {}
      }
      renderPlcSection(brandKey, slaveId);
      return;
    }
    const del = e.target.closest(".plc-del");
    if (del) {
      const idx = Number(del.dataset.idx);
      const list = ensurePlcAlerts(brandKey, slaveId);
      if (idx >= 0 && idx < list.length) {
        list.splice(idx, 1);
        if (typeof saveConfig === "function") {
          try {
            saveConfig();
          } catch (e) {}
        }
        renderPlcSection(brandKey, slaveId);
      }
    }
  });

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    try {
      const trs = Array.from(form.querySelectorAll("tbody tr"));
      const rows = trs.map((tr, i) => ({
        condition:
          tr.querySelector(`select[name="condition_${i}"]`)?.value || "<=",
        threshold:
          tr.querySelector(`input[name="threshold_${i}"]`)?.value || "",
        contact: tr.querySelector(`input[name="contact_${i}"]`)?.value || "",
        message:
          tr.querySelector(`input[name="message_${i}"]`)?.value ||
          `${brandKey} S${slaveId}`,
        enabled:
          tr.querySelector(`input[name="enabled_${i}"]`)?.checked || false,
      }));
      setPlcAlerts(brandKey, slaveId, rows);
      const tick = document.getElementById("plc-tick");
      if (tick) {
        tick.style.display = "inline-block";
        setTimeout(() => (tick.style.display = "none"), 1100);
      }
    } catch (ex) {
      console.error("[PLC] Save failed", ex);
    }
  });
}

// ===================== Keep existing Digital/Analog handlers intact =====================
// setupTabHandlers should continue to attach listeners for Digital I/O and Analog cases,
// and it will not interfere with Modbus because Modbus uses its own containers and forms.

function setupTabHandlers(mainTab, subTab) {
  setTimeout(() => {
    // Digital I/O handlers
    if (mainTab === "Digital I/O") {
      const addBtn = document.getElementById("add-digital-alert-btn");
      if (addBtn) {
        addBtn.onclick = () => {
          const channelStore = config.alarmSettings["Digital I/O"][subTab];
          channelStore.alerts.push({
            trigger: "High",
            contact: "",
            message: "",
            enabled: false,
          });
          renderAlarmSettings("Digital I/O", subTab);
        };
      }

      const form = document.getElementById("digital-io-alerts-form");
      if (form) {
        // Delete row via event delegation
        form.addEventListener("click", (e) => {
          const btn = e.target.closest(".del-alert");
          if (!btn) return;
          const idx = Number(btn.getAttribute("data-index"));
          if (!Number.isInteger(idx)) return;
          const channelStore = config.alarmSettings["Digital I/O"][subTab];
          if (idx >= 0 && idx < channelStore.alerts.length) {
            channelStore.alerts.splice(idx, 1);
            renderAlarmSettings("Digital I/O", subTab);
          }
        });

        // Save alerts
        form.onsubmit = (ev) => {
          ev.preventDefault();
          const channelStore = config.alarmSettings["Digital I/O"][subTab];
          const rows = channelStore.alerts;

          for (let i = 0; i < rows.length; i++) {
            const trigger = form[`trigger_${i}`]?.value;
            const contact = form[`contact_${i}`]?.value;
            const message = form[`message_${i}`]?.value;
            const enabled = form[`enabled_${i}`]?.checked;

            rows[i] = {
              trigger: trigger || "High",
              contact: contact || "",
              message: message || "",
              enabled: !!enabled,
            };
          }

          saveConfig();
          const tick = document.getElementById("digital-io-alerts-tick");
          if (tick) {
            tick.style.display = "inline";
            setTimeout(() => (tick.style.display = "none"), 1200);
          }
        };
      }
    }

    // Modbus handlers
    if (mainTab === "Modbus") {
      const addBtn = document.getElementById("add-modbus-alert-btn");
      if (addBtn) {
        addBtn.onclick = () => {
          const modbusStore = config.alarmSettings.Modbus;
          modbusStore.alerts.push({
            slave_id: "",
            condition: "<=",
            threshold: "",
            contact: "",
            message: "",
            enabled: false,
          });
          renderAlarmSettings("Modbus", "Alerts");
        };
      }

      const form = document.getElementById("modbus-alerts-form");
      if (form) {
        // Delete row via event delegation
        form.addEventListener("click", (e) => {
          const btn = e.target.closest(".del-alert");
          if (!btn) return;
          const idx = Number(btn.getAttribute("data-index"));
          if (!Number.isInteger(idx)) return;
          const modbusStore = config.alarmSettings.Modbus;
          if (idx >= 0 && idx < modbusStore.alerts.length) {
            modbusStore.alerts.splice(idx, 1);
            renderAlarmSettings("Modbus", "Alerts");
          }
        });

        // Save alerts
        form.onsubmit = (ev) => {
          ev.preventDefault();
          const modbusStore = config.alarmSettings.Modbus;
          const rows = modbusStore.alerts;

          for (let i = 0; i < rows.length; i++) {
            const slaveId = form[`slave_id_${i}`]?.value;
            const cond = form[`condition_${i}`]?.value;
            const thr = form[`threshold_${i}`]?.value;
            const contact = form[`contact_${i}`]?.value;
            const msg = form[`message_${i}`]?.value;
            const en = form[`enabled_${i}`]?.checked;

            rows[i] = {
              slave_id:
                slaveId !== undefined && slaveId !== ""
                  ? parseInt(slaveId, 10)
                  : "",
              condition: cond || "<=",
              threshold:
                thr !== undefined && thr !== "" && !Number.isNaN(Number(thr))
                  ? Number(thr)
                  : "",
              contact: contact || "",
              message: msg || "",
              enabled: !!en,
            };
          }

          config.alarmSettings.Modbus.alerts = rows;
          saveConfig();
          const tick = document.getElementById("modbus-alerts-tick");
          if (tick) {
            tick.style.display = "inline";
            setTimeout(() => (tick.style.display = "none"), 1200);
          }
        };
      }
    }

    // Analog handlers
    if (mainTab === "Analog") {
      const addBtn = document.getElementById("add-analog-alert-btn");
      if (addBtn) {
        addBtn.onclick = () => {
          const channelStore = config.alarmSettings.Analog[subTab];
          channelStore.alerts.push({
            condition: "<=",
            threshold: "",
            delay: "",
            contact: "",
            message: "",
            enabled: false,
          });
          renderAlarmSettings("Analog", subTab);
        };
      }

      const form = document.getElementById("analog-alerts-form");
      if (form) {
        // Delete row via event delegation
        form.addEventListener("click", (e) => {
          const btn = e.target.closest(".del-alert");
          if (!btn) return;
          const idx = Number(btn.getAttribute("data-index"));
          if (!Number.isInteger(idx)) return;
          const channelStore = config.alarmSettings.Analog[subTab];
          if (idx >= 0 && idx < channelStore.alerts.length) {
            channelStore.alerts.splice(idx, 1);
            renderAlarmSettings("Analog", subTab);
          }
        });

        // Save alerts
        form.onsubmit = (ev) => {
          ev.preventDefault();
          const channelStore = config.alarmSettings.Analog[subTab];
          const rows = channelStore.alerts;

          for (let i = 0; i < rows.length; i++) {
            const cond = form[`condition_${i}`]?.value;
            const thr = form[`threshold_${i}`]?.value;
            const delay = form[`delay_${i}`]?.value;
            const email = form[`email_${i}`]?.value;
            const contact = form[`contact_${i}`]?.value;
            const msg = form[`message_${i}`]?.value;
            const en = form[`enabled_${i}`]?.checked;

            rows[i] = {
              condition: cond || "<=",
              threshold:
                thr !== undefined && thr !== "" && !Number.isNaN(Number(thr))
                  ? Number(thr)
                  : "",
              delay:
                delay !== undefined &&
                delay !== "" &&
                !Number.isNaN(Number(delay))
                  ? Number(delay)
                  : "",
              email: email || "",
              contact: contact || "",
              message: msg || "",
              enabled: !!en,
            };
          }

          saveConfig();
          const tick = document.getElementById("analog-alerts-tick");
          if (tick) {
            tick.style.display = "inline";
            setTimeout(() => (tick.style.display = "none"), 1200);
          }
        };
      }
    }
  }, 50);
}

function renderOfflineDataPanel() {
  // Inject styles (only once)
  if (!document.getElementById("offline-panel-styles")) {
    const style = document.createElement("style");
    style.id = "offline-panel-styles";
    style.textContent = `
      /* Container */
      #offline-panel-container {
        max-width: 75%;
        margin: 24px auto;
        border-radius: 14px;
        padding: 28px 36px;
        display: flex;
        gap: 36px;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #2e2e2e;
        transition: all 0.3s ease;
      }

      /* Left & Right Sections */
      #offline-panel-left {
        flex: 1;
      }
      #offline-panel-right {
        width: 260px;
        border-radius: 12px;
        height:200px;
        margin-right:500%;
      }

      /* Headings */
      #offline-panel-left h2,
      #offline-panel-right h2 {
        margin-bottom: 18px;
        font-weight: 700;
        color: #333;
        letter-spacing: 0.5px;
        font-size: 18px;
      }

      /* Labels */
      label {
        display: block;
        margin-bottom: 10px;
        font-weight: 600;
        font-size: 14px;
        cursor: pointer;
        color: #444;
      }

      /* Inputs & Select */
      input[type="text"],
      input[type="password"],
      input[type="number"],
      input[type="date"],
      select {
        margin-top: 6px;
        padding: 8px 12px;
        font-size: 14px;
        border-radius: 8px;
        border: 1.5px solid #d0d0d0;
        width: 100%;
        max-width: 240px;
        box-sizing: border-box;
        background-color: #fff;
        transition: border-color 0.25s ease, box-shadow 0.25s ease;
      }
      input:focus,
      select:focus {
        border-color: #4caf50;
        box-shadow: 0 0 0 3px rgba(76,175,80,0.15);
        outline: none;
      }

      /* Radio Buttons */
      input[type="radio"] {
        transform: scale(1.15);
        margin-right: 8px;
        vertical-align: middle;
      }

      /* Section Cards */

      .ftp-schedule{
        display:flex;
        gap:35px;
      }
      .section {
        margin-bottom: 24px;
        padding: 16px 20px;
        width:250px;
        background: #fafafa;
        border-radius: 10px;
        border: 1px solid #eee;
      }
      .section legend {
        font-size: 15px;
        font-weight: 700;
        margin-bottom: 12px;
        color: #4a4a4a;
      }

      /* Buttons */
      button {
        padding: 10px 18px;
        font-size: 14px;
        font-weight: 600;
        border-radius: 8px;
        cursor: pointer;
        border: none;
        color: white;
        background-color: #4caf50;
        transition: background-color 0.3s ease, transform 0.2s ease;
      }
      button:hover {
        background-color: #43a047;
        transform: translateY(-1px);
      }

      /* Log File Controls */
      #logfile-controls {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-top: 8px;
      }
      #logfile-dropdown {
        padding: 8px 12px;
        font-size: 14px;
        border-radius: 6px;
        border: 1.5px solid #ccc;
        min-width: 140px;
        background-color: white;
      }

      /* Button Colors */
      #refresh-btn, #download-btn, #delete-btn {
        background-color: #2196f3;
      }
      #refresh-btn:hover {
        background-color: #1976d2;
      }
      #delete-btn {
        background-color: #f44336;
      }
      #delete-btn:hover {
        background-color: #d32f2f;
      }
      #download-btn:hover {
        background-color: #1565c0;
      }

      /* Status Icon */
      #offline-panel-status {
        font-size: 20px;
        color: #4caf50;
        margin-left: 16px;
        vertical-align: middle;
        display: none;
      }

      /* Schedule section hide */
      #schedule-section.hidden {
        display: none;
      }

      /* Responsive */
      @media (max-width: 780px) {
        #offline-panel-container {
          flex-direction: column;
          padding: 20px;
        }
        #offline-panel-right {
          width: 100%;
          margin-top: 20px;
        }
      }
    `;

    document.head.appendChild(style);
  }

  const offlineCfg = config.offlineData || {
    enabled: true,
    mode: "schedule", // or "live"
    ftp: {
      server: "FTPserver",
      user: "ftpusername",
      pass: "",
      port: 8080,
      folder: "datalogger",
    },
    schedule: {
      hour: 4,
      min: 15,
      sec: 0,
    },
  };
  const today = new Date().toISOString().split("T")[0];
  if (offlineCfg.schedule.date == null || offlineCfg.schedule.date < today)
    offlineCfg.schedule.date = today; // Default to today
  let logFiles = []; // to be loaded

  document.getElementById("main-panel").innerHTML = `
        <div id="offline-panel-container">
          <div id="offline-panel-left">
            <form id="offline-form">
              <div class="section">
                <legend>Offline File Upload</legend>
                <label><input type="radio" name="enabled" value="true" ${
                  offlineCfg.enabled ? "checked" : ""
                }> Enable</label>
                <label><input type="radio" name="enabled" value="false" ${
                  !offlineCfg.enabled ? "checked" : ""
                }> Disable</label>
              </div>
              <div class="section">
                <legend>Mode</legend>
                <label><input type="radio" name="mode" value="live" ${
                  offlineCfg.mode === "live" ? "checked" : ""
                }> Live</label>
                <label><input type="radio" name="mode" value="schedule" ${
                  offlineCfg.mode === "schedule" ? "checked" : ""
                }> Schedule</label>
              </div>
              <section class="ftp-schedule">
              <div class="section">
                <legend>FTP Settings</legend>
                <label>FTP Server IP: <input type="text" name="ftpserver" value="${
                  offlineCfg.ftp.server
                }"></label>
                <label>Username: <input type="text" name="ftpuser" value="${
                  offlineCfg.ftp.user
                }"></label>
                <label>Password: <input type="password" name="ftppass" value="${
                  offlineCfg.ftp.pass
                }"></label>
                <label>Port Number: <input type="number" name="ftpport" value="${
                  offlineCfg.ftp.port
                }"></label>
                <label>Backup Folder: <input type="text" name="ftpfolder" value="${
                  offlineCfg.ftp.folder
                }"></label>
              </div>
              <div class="section" id="schedule-section">
                <legend>Schedule</legend>
                <div>
                  Time: h:
                  <input type="number" name="hour" min="0" max="23" value="${
                    offlineCfg.schedule.hour
                  }" style="width: 50px;">
                  m:
                  <input type="number" name="min" min="0" max="59" value="${
                    offlineCfg.schedule.min
                  }" style="width: 50px;">
                  s:
                  <input type="number" name="sec" min="0" max="59" value="${
                    offlineCfg.schedule.sec
                  }" style="width: 50px;">
                  (24 Hr Format)
                </div>
                <div style="margin-top: 12px;">
                  Date: <input type="date" name="date" value="${
                    offlineCfg.schedule.date
                  }">
                </div>
              </div>
              </section>
              <div style="margin-top: 20px;">
                <button type="submit" class="button primary">Read</button>
                <span id="offline-panel-status">✔</span>
              </div>
            </form>
          </div>
          <div id="offline-panel-right">
            <h2>Offline Log File</h2>
            <form id="logfile-form" style="display: flex; align-items: center; gap: 12px;">
              <select id="logfile-dropdown"></select>
              <button type="button" id="refresh-btn" title="Refresh">↻</button>
              <button type="button" id="download-btn">Download</button>
              <button type="button" id="delete-btn">Delete</button>
            </form>
          </div>
        </div>
      `;

  // Fetch and load log files into dropdown
  function loadLogFiles() {
    fetch("/api/logfiles")
      .then((res) => res.json())
      .then((files) => {
        logFiles = files || [];
        const dropdown = document.getElementById("logfile-dropdown");
        dropdown.innerHTML = logFiles
          .map((f) => `<option value="${f}">${f}</option>`)
          .join("");
      });
  }
  loadLogFiles();

  document.getElementById("refresh-btn").onclick = loadLogFiles;
  document.getElementById("download-btn").onclick = () => {
    const sel = document.getElementById("logfile-dropdown");
    if (!sel.value) return;
    window.open(`/api/logfile/${encodeURIComponent(sel.value)}`);
  };
  document.getElementById("delete-btn").onclick = () => {
    const sel = document.getElementById("logfile-dropdown");
    if (!sel.value) return;
    if (!confirm(`Delete log file "${sel.value}"?`)) return;
    fetch(`/api/logfile/${encodeURIComponent(sel.value)}`, {
      method: "DELETE",
    }).then(() => {
      alert("File deleted");
      loadLogFiles();
    });
  };

  // Hide schedule when mode is set to live
  const modeRadios = document.querySelectorAll('input[name="mode"]');
  const scheduleSection = document.getElementById("schedule-section");
  function updateScheduleVisibility() {
    const mode = document.querySelector('input[name="mode"]:checked').value;
    scheduleSection.style.display = mode === "schedule" ? "block" : "none";
  }
  modeRadios.forEach((radio) =>
    radio.addEventListener("change", updateScheduleVisibility)
  );
  updateScheduleVisibility(); // initial call

  // Handle form submission (Read button)
  document.getElementById("offline-form").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const form = ev.target;

    // --- Build selected date+time ---
    const selectedDateTime = new Date(
      `${form.date.value}T${form.hour.value.padStart(
        2,
        "0"
      )}:${form.min.value.padStart(2, "0")}:${form.sec.value.padStart(2, "0")}`
    );

    // --- Build min allowed date+time from offlineCfg ---
    const cfg = offlineCfg.schedule;
    const minAllowedDateTime = new Date(
      `${cfg.date}T${String(cfg.hour).padStart(2, "0")}:${String(
        cfg.min
      ).padStart(2, "0")}:${String(cfg.sec).padStart(2, "0")}`
    );

    // --- Compare ---
    if (selectedDateTime < minAllowedDateTime) {
      alert(
        "Selected date/time cannot be earlier than the configured schedule!"
      );
      return; // cancel save
    }

    // --- If OK, proceed ---
    const payload = {
      offlineData: {
        enabled: form.enabled.value === "true",
        mode: form.mode.value,
        ftp: {
          server: form.ftpserver.value,
          user: form.ftpuser.value,
          pass: form.ftppass.value,
          port: parseInt(form.ftpport.value, 10),
          folder: form.ftpfolder.value,
        },
        schedule: {
          hour: parseInt(form.hour.value, 10),
          min: parseInt(form.min.value, 10),
          sec: parseInt(form.sec.value, 10),
          date: form.date.value,
        },
      },
    };

    config.offlineData = payload.offlineData;
    saveConfig();

    const status = document.getElementById("offline-panel-status");
    status.style.display = "inline";
    setTimeout(() => (status.style.display = "none"), 2000);
  });
}

function file_to_db() {
  let fileCount = 0;
  const main = document.getElementById("main-panel");

  // Panel skeleton with CSS for scroll behavior
  main.innerHTML = `
        <div class="panel-header">File to Database</div>
        <style>
          .file-content {
            scrollbar-width: thin;
            scrollbar-color: #007bff #f1f1f1;
            box-sizing: border-box;
            scrollbar-gutter: stable;
          }
          .file-content::-webkit-scrollbar {
            width: 8px;
          }
          .file-content::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 4px;
          }
          .file-content::-webkit-scrollbar-thumb {
            background: #007bff;
            border-radius: 4px;
          }
          .file-content::-webkit-scrollbar-thumb:hover {
            background: #0056b3;
          }
        </style>
        <div style="padding:15px;">
          <div id="files-container">
            <!-- File configurations will be added here -->
          </div>
          <button type="button" class="button-primary" id="add-file-btn">Add File</button>
          <br><br>
          <button type="submit" class="button-primary" id="process-files-btn">Process Files</button>
          <span id="process-tick" style="display:none;font-size:18px;color:#49ba3c;">✓</span>
        </div>
      `;

  // Helpers
  function getFilenameWithoutExtension(filePath) {
    const filename = (filePath || "").split(/[\\\/]/).pop() || "";
    return filename.replace(/\.[^/.]+$/, "");
  }

  // Add a complete file configuration panel
  function addFileConfiguration(configData = null) {
    fileCount++;
    const fileId = `file-config-${fileCount}`;

    // Extract values
    const enabled = configData?._internal?.enabled !== false;
    const collapsed = configData?._internal?.collapsed || false;

    const dataFreq = configData?.["data_freq(in secs)"] ?? "";
    const smbShare = configData?.SMBShare?.smb_share || "";
    const shareUsername = configData?.SMBShare?.share_username || "";
    const sharePassword = configData?.SMBShare?.share_password || "";

    const fileHtml = `
          <div class="file-config" id="${fileId}" style="margin-bottom:20px;border:2px solid #007bff;border-radius:8px;background:#f8f9fa;">
            <div class="file-header" style="display:flex;justify-content:space-between;align-items:center;padding:15px;cursor:pointer;" onclick="toggleFileContent('${fileId}')">
              <div style="display:flex;align-items:center;gap:10px;">
                <span class="collapse-icon" style="font-size:18px;transition:transform 0.3s ease;transform:rotate(0deg);">▼</span>
                <h4 style="margin:0;color:#007bff;">File Configuration #${fileCount}</h4>
              </div>
              <div style="display:flex;align-items:center;gap:10px;" onclick="event.stopPropagation();">
                <label style="font-weight:bold;">Enable:</label>
                <label class="toggle-switch" style="position:relative;display:inline-block;width:50px;height:24px;">
                  <input type="checkbox" class="file-enabled" ${
                    enabled ? "checked" : ""
                  } style="opacity:0;width:0;height:0;">
                  <span class="slider" style="position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background-color:#ccc;transition:.4s;border-radius:24px;"></span>
                </label>
              </div>
            </div>

            <div class="file-content" style="max-height:${
              collapsed ? "0px" : "500px"
            };overflow-y:auto;overflow-x:hidden;padding:${
      collapsed ? "0 15px" : "0 15px 15px 15px"
    };opacity:${collapsed ? "0" : "1"};transition:all 0.3s ease;">
              <div class="file-form">
                <!-- SMB Details -->
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px;">
                  <div>
                    <label style="display:block;margin-bottom:5px;font-weight:bold;">SMB Share:</label>
                    <input type="text" class="smb-share" placeholder="//server/share" value="${smbShare}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                  </div>
                  <div>
                    <label style="display:block;margin-bottom:5px;font-weight:bold;">Data Frequency (in secs):</label>
                    <input type="number" class="data-freq" placeholder="60" value="${dataFreq}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                  </div>
                </div>

                <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px;">
                  <div>
                    <label style="display:block;margin-bottom:5px;font-weight:bold;">Share Username:</label>
                    <input type="text" class="share-username" placeholder="username" value="${shareUsername}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                  </div>
                  <div>
                    <label style="display:block;margin-bottom:5px;font-weight:bold;">Share Password:</label>
                    <input type="password" class="share-password" placeholder="password" value="${sharePassword}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                  </div>
                </div>
                
                <button type="button" class="remove-file-btn" style="background:#dc3545;color:white;border:none;padding:10px 20px;border-radius:4px;cursor:pointer;margin-bottom:10px;">Remove File Configuration</button>
              </div>
            </div>
          </div>
        `;

    document
      .getElementById("files-container")
      .insertAdjacentHTML("beforeend", fileHtml);

    const fileConfig = document.getElementById(fileId);
    if (!fileConfig) {
      console.error(`File config element ${fileId} not found`);
      return;
    }

    // Toggle switch behavior
    const slider = fileConfig.querySelector(".slider");
    const checkbox = fileConfig.querySelector(".file-enabled");
    if (checkbox && slider) {
      checkbox.onchange = () => {
        slider.style.backgroundColor = checkbox.checked ? "#007bff" : "#ccc";
        const fileForm = fileConfig.querySelector(".file-form");
        if (fileForm) {
          fileForm.style.opacity = checkbox.checked ? "1" : "0.5";
          fileForm.style.pointerEvents = checkbox.checked ? "auto" : "none";
        }
      };
      slider.style.backgroundColor = checkbox.checked ? "#007bff" : "#ccc";
      slider.innerHTML =
        '<span style="position:absolute;content:"";height:18px;width:18px;left:3px;bottom:3px;background-color:white;transition:.4s;border-radius:50%;transform:translateX(' +
        (checkbox.checked ? "26px" : "0px") +
        ');"></span>';
      const fileForm = fileConfig.querySelector(".file-form");
      if (fileForm) {
        fileForm.style.opacity = checkbox.checked ? "1" : "0.5";
        fileForm.style.pointerEvents = checkbox.checked ? "auto" : "none";
      }
    }

    // Remove file configuration
    const removeFileBtn = fileConfig.querySelector(".remove-file-btn");
    if (removeFileBtn) {
      removeFileBtn.onclick = () => {
        fileConfig.remove();
      };
    }
  }

  // Collapse/expand
  window.toggleFileContent = function (fileId) {
    const fileConfig = document.getElementById(fileId);
    if (!fileConfig) return;
    const content = fileConfig.querySelector(".file-content");
    const icon = fileConfig.querySelector(".collapse-icon");
    if (!content || !icon) return;
    const isCollapsed =
      content.style.maxHeight === "0px" || content.style.opacity === "0";
    if (!isCollapsed) {
      content.style.maxHeight = "0px";
      content.style.opacity = "0";
      content.style.padding = "0 15px";
      icon.style.transform = "rotate(-90deg)";
    } else {
      content.style.maxHeight = "500px";
      content.style.opacity = "1";
      content.style.padding = "0 15px 15px 15px";
      icon.style.transform = "rotate(0deg)";
    }
  };

  // Save configuration
  function saveCurrentConfigurationState() {
    const fileConfigs = document.querySelectorAll(".file-config");
    const filesToProcess = [];
    fileConfigs.forEach((configElement) => {
      const content = configElement.querySelector(".file-content");
      const isCollapsed =
        content &&
        (content.style.maxHeight === "0px" || content.style.opacity === "0");
      const enabled =
        configElement.querySelector(".file-enabled")?.checked || false;

      const smbShare = configElement.querySelector(".smb-share")?.value || "";
      const shareUsername =
        configElement.querySelector(".share-username")?.value || "";
      const sharePassword =
        configElement.querySelector(".share-password")?.value || "";

      const dataFreqVal = parseInt(
        configElement.querySelector(".data-freq")?.value
      );
      const dataFreqSecs = Number.isFinite(dataFreqVal) ? dataFreqVal : 60;

      const configData = {
        SMBShare: {
          smb_share: smbShare,
          share_username: shareUsername,
          share_password: sharePassword,
        },
        storing_database: {
          table_name: getFilenameWithoutExtension(smbShare), // Use SMB share for filename
        },
        processed_files_table: "",
        "data_freq(in secs)": dataFreqSecs,
        _internal: {
          enabled: enabled,
          collapsed: isCollapsed,
        },
      };

      filesToProcess.push(configData);
    });

    config.fileToDb = { files: filesToProcess };
    return saveConfig();
  }

  // Initialize with saved config
  async function initializeWithConfig() {
    const fileToDbConfig = config.fileToDb || { files: [] };
    if (fileToDbConfig.files && fileToDbConfig.files.length > 0) {
      fileToDbConfig.files.forEach((fileConfigData) => {
        addFileConfiguration(fileConfigData);
      });
    } else {
      addFileConfiguration();
    }
  }

  // Wire buttons
  setTimeout(() => {
    const addFileBtn = document.getElementById("add-file-btn");
    if (addFileBtn) {
      addFileBtn.onclick = () => addFileConfiguration();
    }
    const processFilesBtn = document.getElementById("process-files-btn");
    if (processFilesBtn) {
      processFilesBtn.onclick = async () => {
        await saveCurrentConfigurationState();
        const tick = document.getElementById("process-tick");
        if (tick) {
          tick.style.display = "inline";
          setTimeout(() => (tick.style.display = "none"), 1500);
        }
        console.log("Configuration saved and files processed");
      };
    }
  }, 100);

  initializeWithConfig();
}

function renderKafkaPanel() {
  const kafkaConfig = config.kafka || {
    brokers: ["broker1:9092"],
    topic: "",
    certFiles: {
      ca: "",
      cert: "",
      key: "",
    },
  };

  document.getElementById("main-panel").innerHTML = `
        <h2>Kafka Configuration</h2>
        <form id="kafka-form" enctype="multipart/form-data" novalidate>
          <label>Brokers (comma-separated):<br>
            <input type="text" name="brokers" style="width:100%" value="${kafkaConfig.brokers.join(
              ","
            )}">
          </label><br><br>

          <label>Topic:<br>
            <input type="text" name="topic" style="width:100%" value="${
              kafkaConfig.topic
            }">
          </label><br><br>
    <fieldset>
      <legend>Certificates</legend>

      <label>CA Certificate:</label>
      ${
        kafkaConfig.certFiles.ca
          ? `<div>
              <small>${kafkaConfig.certFiles.ca}</small>
              <button type="button" class="change-btn" data-field="ca">Change</button>
            </div>`
          : `<input type="file" name="ca" id="caFile">`
      }
      <br>

      <label>Client Certificate:</label>
      ${
        kafkaConfig.certFiles.cert
          ? `<div>
              <small>${kafkaConfig.certFiles.cert}</small>
              <button type="button" class="change-btn" data-field="cert">Change</button>
            </div>`
          : `<input type="file" name="cert" id="certFile">`
      }
      <br>

      <label>Client Key:</label>
      ${
        kafkaConfig.certFiles.key
          ? `<div>
              <small>${kafkaConfig.certFiles.key}</small>
              <button type="button" class="change-btn" data-field="key">Change</button>
            </div>`
          : `<input type="file" name="key" id="keyFile">`
      }
      <br>
    </fieldset>

          <br>

          <button type="submit" class="button-primary">Save</button>
          <span id="kafka-save-status" style="margin-left:1em; color: green;"></span>
        </form>
      `;

  const form = document.getElementById("kafka-form");

  form.onsubmit = async (ev) => {
    ev.preventDefault();

    const brokers = form.brokers.value
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const topic = form.topic.value.trim();

    // File uploads
    const certFiles = {
      ca: kafkaConfig.certFiles.ca,
      cert: kafkaConfig.certFiles.cert,
      key: kafkaConfig.certFiles.key,
    };
    const uploadFile = async (fileInputId, key) => {
      const input = document.getElementById(fileInputId);
      if (input && input.files.length > 0) {
        const fileData = new FormData();
        fileData.append("file", input.files[0]);
        const resp = await fetch("/upload-kafka-cert/", {
          method: "POST",
          body: fileData,
        });
        if (!resp.ok) throw new Error(`Failed to upload ${key}`);
        const json = await resp.json();
        console.log(json);

        certFiles[key] = json.path;
        kafkaConfig.certFiles[key] = json.path; // <-- keep global config in sync
      }
    };

    try {
      // Upload cert files if selected
      await Promise.all([
        uploadFile("caFile", "ca"),
        uploadFile("certFile", "cert"),
        uploadFile("keyFile", "key"),
      ]);

      // Prepare config save payload
      const kafka = {
        brokers,
        topic,
        certFiles,
      };
      config.kafka = kafka; // Update global config
      saveConfig(); // Save to config.json

      document.getElementById("kafka-save-status").textContent = "Saved ✓";
      setTimeout(() => {
        document.getElementById("kafka-save-status").textContent = "";
      }, 3000);
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  };

  document.querySelectorAll(".change-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const field = btn.getAttribute("data-field");

      // Clear the stored filename in kafkaConfig
      kafkaConfig.certFiles[field] = "";

      // Replace the display with a file input
      btn.parentElement.outerHTML = `<input type="file" name="${field}" id="${field}File">`;
    });
  });
}

function renderAddUserPanel(currentRole) {
  const allowed = {
    superadmin: ["superadmin", "admin", "user"],
    admin: ["user"],
    user: [],
  };

  const main = document.getElementById("main-panel");

  if (allowed[currentRole].length === 0) {
    main.innerHTML = `<div class="panel-header">Add User</div>
                          <p style="padding:15px;">You are not allowed to create new users.</p>`;
    return;
  }

  main.innerHTML = `
        <div class="panel-header">Add User</div>
        <div style="padding:15px;">
          <form id="add-user-form">
            <label>Username:
              <input type="text" name="username" required style="margin-left:10px;">
            </label>
            <br><br>
            <label>Password:
              <input type="password" name="password" required style="margin-left:10px;">
            </label>
            <br><br>
            <label>Role:
              <select name="role" style="margin-left:10px;">
                ${allowed[currentRole]
                  .map((r) => `<option value="${r}">${r}</option>`)
                  .join("")}
              </select>
            </label>
            <br><br>
            <button type="submit" class="button-primary">Create User</button>
            <span id="add-user-tick" style="display:none;font-size:18px;color:#49ba3c;">&#10004;</span>
          </form>
        </div>
      `;

  document.getElementById("add-user-form").onsubmit = async (ev) => {
    ev.preventDefault();
    const data = {
      username: ev.target.username.value,
      password: ev.target.password.value,
      role: ev.target.role.value,
    };

    const res = await fetch("/create-user", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });

    if (res.ok) {
      document.getElementById("add-user-tick").style.display = "inline";
      setTimeout(
        () => (document.getElementById("add-user-tick").style.display = "none"),
        1200
      );
    } else {
      const err = await res.json();
      alert("Error: " + err.error);
    }
  };
}

function logoutUser() {
  fetch("/logout", {
    method: "POST",
    credentials: "include", // send cookies so backend can delete
    headers: {
      "Content-Type": "application/json",
    },
  })
    .then((res) => res.json())
    .then((data) => {
      if (data.status === "logged_out") {
        // Optionally redirect to login page
        window.location.href = "/";
      } else {
        alert("Logout failed");
      }
    })
    .catch((err) => {
      console.error("Logout error:", err);
      alert("Error logging out");
    });
}

// Side menu navigation logic
document.querySelectorAll("#side-nav li").forEach((li) => {
  li.onclick = () => {
    // Highlight selected
    document
      .querySelectorAll("#side-nav li")
      .forEach((l) => l.classList.remove("active"));
    li.classList.add("active");
    let panel = li.getAttribute("data-panel");
    if (panel === "io-settings") renderIOSettingsPanel("Settings");
    else if (panel === "modbus-rtu") renderModBusRTUPanel();
    else if (panel === "modbus-tcp") renderModbusTcpPanel();
    else if (panel === "Wifi/4G") renderNetworkPanel();
    else if (panel === "alarm") renderAlarmSettings();
    else if (panel === "manual") renderOfflineDataPanel();
    else if (panel === "file-to-db") file_to_db();
    else if (panel === "kafka") renderKafkaPanel();
    else if (panel === "add-user") {
      fetch("/whoami")
        .then((res) => res.json())
        .then((data) => {
          if (data.role) {
            renderAddUserPanel(data.role);
          } else {
            alert("Unauthorized");
          }
        });
    } else if (panel === "change-password") renderChangePasswordPanel();
    else if (panel === "logout") {
      if (confirm("Are you sure you want to log out?")) {
        logoutUser(); // calls the function above
      }
    } else if (panel === "help")
      renderSimplePanel(
        "Help",
        "For help, contact support.<br>Version: <b>v1.2.5</b>"
      );
  };
});

// Simple panel renderer for Logout/Help
function renderSimplePanel(title, content) {
  document.getElementById(
    "main-panel"
  ).innerHTML = `<div class="panel-header">${title}</div><div class="tab-content">${content}</div>`;
}

// Show green checkmark briefly
function showTick(id) {
  const el = document.getElementById(id);
  el.style.display = "inline";
  setTimeout(() => {
    el.style.display = "none";
  }, 1200);
}

// Download config.json helper
function downloadConfig() {
  const blob = new Blob([JSON.stringify(config, null, 2)], {
    type: "application/json",
  });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "config.json";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

// Initial panel
window.onload = loadConfig;
