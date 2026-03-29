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

function isReadOnly() {
  return uiState.role === "user" || uiState.role === "guest";
}

async function loadConfig() {
  const res = await fetch("/config");
  const cfg = await res.json();
  config = cfg;
  renderIOSettingsPanel();
}

async function saveConfig() {
  if (isReadOnly()) {
    alert("User can only view. Changes are not allowed.");
    return;
  }
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
if (!config.ioSettings.analog)
  config.ioSettings.analog = {
    channels: [
      {
        enabled: true,
        mode: "0.-2.048V",
        range: "0-5V",
        resolution: "16 bit",
      },
    ],
    pollingInterval: 5,
    pollingIntervalUnit: "Sec",
  };
if (!config.ioSettings.analog.db)
  config.ioSettings.analog.db = {
    upload_local: true,
    upload_cloud: false,
    db_name: "",
    table_name: "",
  };

if (!config.ioSettings.digitalInput)
  config.ioSettings.digitalInput = {
    channels: [true, true],
  };

if (!config.ioSettings.digitalOutput)
  config.ioSettings.digitalOutput = {
    channels: [0, 0],
  };

if (!config.ioSettings.digitalInput.db)
  config.ioSettings.digitalInput.db = {
    upload_local: true,
    upload_cloud: false,
    db_name: "",
    table_name: "",
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

function updateAnalogRange(index, selected = "") {
  const modeSelect = document.querySelector(
    `select[name="ai_mode_${index}"]`
  );
  const rangeSelect = document.getElementById(`ch_range_${index}`);

  if (!modeSelect || !rangeSelect) return;

  const mode = modeSelect.value;

  let options = [];

  if (mode === "0.-2.048V") {
    options = [
      { v: "0-10", l: "0–10 V" },
      { v: "0-5", l: "0–5 V" },
      { v: "0-2", l: "0–2 V" },
    ];
  } else if (mode === "4-20mA") {
    options = [
      { v: "4-20", l: "4–20 mA" },
      { v: "0-20", l: "0–20 mA" },
    ];
  }

  rangeSelect.innerHTML = "";

  options.forEach(o => {
    const opt = document.createElement("option");
    opt.value = o.v;
    opt.textContent = o.l;
    if (o.v === selected) opt.selected = true;
    rangeSelect.appendChild(opt);
  });
}


function isPrivilegedRole() {
  return uiState.role === "admin" || uiState.role === "superadmin";
}

function renderDBSettings(prefix, db, opts = {}) {
  db = db || {
    upload_local: true,
    upload_cloud: false,
    db_name: "",
    table_name: "",
  };

  const canSeeDbFields = isPrivilegedRole();

  return `
    <div style="margin-top:12px;padding:8px;border:1px solid #ccc;">
      <b>Database Settings</b>

      <div style="display:flex;gap:12px;align-items:center;margin-top:6px;">
        <label>
          <input type="checkbox" name="${prefix}_upload_local"
            ${db.upload_local ? "checked" : ""}>
          Local DB
        </label>

        <label>
          <input type="checkbox" name="${prefix}_upload_cloud"
            ${db.upload_cloud ? "checked" : ""}>
          Cloud DB
        </label>
${canSeeDbFields
      ? `
      <label>
              <b>Database Name</b>
      <input type="text"
        name="${prefix}_db_name"
        placeholder="Database name"
        value="${db.db_name || "test"}"
        style="width:140px;">

        </label>

        <label>
                <b>Table Name</b>
      <input type="text"
        name="${prefix}_table_name"
        placeholder="Table name"
        value="${db.table_name || ""}"
        style="width:140px;">
        </label>
    `
      : `
      <input type="hidden"
        name="${prefix}_db_name"
        value="${db.db_name || "test"}">

      <input type="hidden"
        name="${prefix}_table_name"
        value="${db.table_name || ""}">
    `

    }
      </div>
    </div>
  `;
}


function normalizeDigitalSchema() {
  const io = config.ioSettings;

  // ---- Digital I/O ----
  if (Array.isArray(io.digitalInput.channels)) {
    io.digitalInput.channels = io.digitalInput.channels.map((ch, i) => {
      if (typeof ch === "boolean") {
        return { name: `DI${i + 1}`, enabled: ch };
      }
      return ch;
    });
  }

  // ---- Digital Output ----
  if (Array.isArray(io.digitalOutput.channels)) {
    io.digitalOutput.channels = io.digitalOutput.channels.map((ch, i) => {
      if (typeof ch === "number") {
        return { name: `DO${i + 1}`, state: ch };
      }
      return ch;
    });
  }
}

function addDI() {
  config.ioSettings.digitalInput.channels.push({
    name: `DI${config.ioSettings.digitalInput.channels.length + 1}`,
    enabled: false,
    pin: "",
  });
  renderIOSettingsPanel("Digital I/O");
}

function removeDI(index) {
  config.ioSettings.digitalInput.channels.splice(index, 1);
  renderIOSettingsPanel("Digital I/O");
}

function addDO() {
  config.ioSettings.digitalOutput.channels.push({
    name: `DO${config.ioSettings.digitalOutput.channels.length + 1}`,
    state: 0,
    pin: "",
  });
  renderIOSettingsPanel("Digital I/O");
}

function removeDO(index) {
  config.ioSettings.digitalOutput.channels.splice(index, 1);
  renderIOSettingsPanel("Digital I/O");
}

function addAI() {
  config.ioSettings.analog.channels.push({
    name: `AI${config.ioSettings.analog.channels.length + 1}`,
    enabled: false,
    mode: "0.-2.048V",
    range: "",
    address: "",
  });
  renderIOSettingsPanel("Analog I/O");
}

function addAO() {
  config.ioSettings.analogOutput.channels.push({
    name: `AO${config.ioSettings.analogOutput.channels.length + 1}`,
    enabled: false,
    mode: "0-10V",
    range: "",
    address: ""
  });
  renderIOSettingsPanel("Analog I/O");
}

function removeAO(index) {
  config.ioSettings.analogOutput.channels.splice(index, 1);
  renderIOSettingsPanel("Analog I/O");
}


function removeAI(index) {
  config.ioSettings.analog.channels.splice(index, 1);
  renderIOSettingsPanel("Analog I/O");
}

function getAnalogMin(range) {
  if (!range || !range.includes(":")) return "";
  const [min] = range.split(":");
  return min;
}

function getAnalogMax(range) {
  const [min, max] = range
  .match(/-?\d+(\.\d+)?/g)
  ?.map(Number) || [];
  return max;
}

function addRelay() {
  config.ioSettings.digitalRelay.channels.push({
    name: `R${config.ioSettings.digitalRelay.channels.length + 1}`,
    enabled: false,
    pin: "",
    mode: "NO"
  });
  renderIOSettingsPanel("Digital I/O");
}

function removeRelay(index) {
  config.ioSettings.digitalRelay.channels.splice(index, 1);
  renderIOSettingsPanel("Digital I/O");
}


function renderIOSettingsPanel(subTab = "Settings") {
  const io = config.ioSettings;
  normalizeDigitalSchema();

  /* ---------- SAFETY DEFAULTS ---------- */
  io.analog ??= {};
  io.analogOutput ??= {};
io.analogOutput.channels ??= [
  { name: "AO1", enabled: false, mode: "0-10V", range: "", address: "" }
];

  io.analog.pollingInterval ??= 5;
  io.analog.pollingIntervalUnit ??= "Sec";
  io.analog.channels ??= [
    { name: "AI1", enabled: false, mode: "0.-2.048V", range: "", address: "" },
  ];
  io.analog.db ??= {
    upload_local: true,
    upload_cloud: false,
    db_name: "",
    table_name: "",
  };

  io.digitalInput ??= {};
  io.digitalInput.pollingInterval ??= 1;
  io.digitalInput.pollingIntervalUnit ??= "Sec";
  io.digitalInput.channels ??= [
    { name: "DI1", enabled: false, pin: "" },
    { name: "DI2", enabled: false, pin: "" },
  ];
  io.digitalInput.db ??= {
    upload_local: true,
    upload_cloud: false,
    db_name: "",
    table_name: "",
  };

  io.digitalOutput ??= {};
  io.digitalOutput.channels ??= [
    { name: "DO1", state: 0, pin: "" },
    { name: "DO2", state: 0, pin: "" },
  ];

  io.digitalRelay ??= {};
io.digitalRelay.channels ??= [
  { name: "R1", enabled: false, pin: "", mode: "NO" }
];


let html = `
  <div class="panel-header">I/O Settings</div>

  <div class="tab-list">
    ${["Settings", "Analog I/O", "Digital I/O", "Modbus RTU", "Modbus TCP"]
      .map(
        (t) => `
          <button class="tab-btn ${subTab === t ? "active" : ""}"
                  data-tab="${t}">
            ${t}
          </button>`
      )
      .join("")}
  </div>

  <div class="tab-content">
    <div id="io-tab-mount"></div>
  </div>
`;




  html += `</div>`;
  document.getElementById("main-panel").innerHTML = html;
  const mount = document.getElementById("io-tab-mount");
if (!mount) {
  console.error("io-tab-mount not found");
  return;
}


  /* ================= SETTINGS ================= */
  if (subTab === "Settings") {
    mount.innerHTML = `
      <form id="io-settings-form">
        <label><input type="checkbox" name="modbus" ${io.settings.modbus ? "checked" : ""}> Modbus RTU</label>
        <label><input type="checkbox" name="modbusTCP" ${io.settings.modbusTCP ? "checked" : ""}> Modbus TCP</label>
        <label><input type="checkbox" name="analog" ${io.settings.analog ? "checked" : ""}> Analog I/O</label>
        <label><input type="checkbox" name="digitalInput" ${io.settings.digitalInput ? "checked" : ""}> Digital I/O</label>
        <br><br>
        <button class="button-primary" type="submit">Save</button>
      </form>
    `;
}

/* ================= ANALOG ================= */
if (subTab === "Analog I/O") {
  const a = io.analog;

    mount.innerHTML = `
<form id="analog-form">
<h3>Analog Input</h3>

<label><input type="radio" name="intervalUnit" value="Sec" ${a.pollingIntervalUnit === "Sec" ? "checked" : ""}> Sec</label>
<label><input type="radio" name="intervalUnit" value="Min" ${a.pollingIntervalUnit === "Min" ? "checked" : ""}> Min</label>
<label><input type="radio" name="intervalUnit" value="Hour" ${a.pollingIntervalUnit === "Hour" ? "checked" : ""}> Hour</label>

&nbsp; Polling:
<input type="number" name="pollingInterval" value="${a.pollingInterval}" min="1" style="width:70px;">

<br><br>

<table class="channel-table">
<tr>
  <th>Channel</th>
  <th>Name</th>
  <th>Enable</th>
  <th>Electrical Range</th>
<th>Process Min</th>
<th>Process Max</th>

  <th>Address</th>
  <th></th>
</tr>

${a.channels.map((ch, i) => `
<tr>
  <td>AI ${i + 1}</td>
  <td><input name="ai_name_${i}" value="${ch.name}"></td>
  <td><input type="checkbox" name="ai_enable_${i}" ${ch.enabled ? "checked" : ""}></td>

  <td>
    <select name="ai_mode_${i}" onchange="updateAnalogRange(${i})">
      <option value="0.-2.048V" ${ch.mode === "0.-2.048V" ? "selected" : ""}>0–2.048V</option>
      <option value="4-20mA" ${ch.mode === "4-20mA" ? "selected" : ""}>4–20mA</option>
    </select>
  </td>
<td>
<input type="number"
  step="any"
  name="ai_min_${i}"
  value="${getAnalogMin(ch.range)}"
  style="width:90px;">
</td>

<td>
<input type="number"
  step="any"
  name="ai_max_${i}"
  value="${getAnalogMax(ch.range)}"
  style="width:90px;">
</td>

  <td>
    <input name="ai_address_${i}" value="${ch.address ?? ""}" placeholder="Addr">
  </td>

  <td>
    <button type="button" onclick="removeAI(${i})">✕</button>
  </td>
</tr>
`).join("")}

</table>

<button type="button" onclick="addAI()">+ Add Analog Input</button>


<h3 style="margin-top:20px;">Analog Output</h3>

<table class="channel-table">
<tr>
<th>Channel</th>
<th>Name</th>
<th>Enable</th>
<th>Electrical Range</th>
<th>Process Min</th>
<th>Process Max</th>
<th>Address</th>
<th></th>
</tr>

${io.analogOutput.channels.map((ch, i) => `
<tr>
<td>AO ${i + 1}</td>
<td><input name="ao_name_${i}" value="${ch.name}"></td>
<td><input type="checkbox" name="ao_enable_${i}" ${ch.enabled ? "checked" : ""}></td>

<td>
  <select name="ao_mode_${i}">
    <option value="0-10V" ${ch.mode === "0-10V" ? "selected" : ""}>0–10V</option>
    <option value="4-20mA" ${ch.mode === "4-20mA" ? "selected" : ""}>4–20mA</option>
  </select>
</td>

<td><input type="number" step="any" name="ao_min_${i}" value="${getAnalogMin(ch.range)}"></td>
<td><input type="number" step="any" name="ao_max_${i}" value="${getAnalogMax(ch.range)}"></td>

<td><input name="ao_address_${i}" value="${ch.address || ""}"></td>

<td><button type="button" onclick="removeAO(${i})">✕</button></td>
</tr>
`).join("")}
</table>

<button type="button" onclick="addAO()">+ Add Analog Output</button>


${renderDBSettings("analog", a.db)}

<br>
<button class="button-primary" type="submit">Save</button>
</form>`;
}


/* ================= DIGITAL ================= */
if (subTab === "Digital I/O") {
  const di = io.digitalInput;
  const doo = io.digitalOutput;

    mount.innerHTML = `
<form id="digital-io-form">

<h3>Digital Input</h3>

<label>
  Polling:
  <input type="number" name="digitalPollingInterval"
         value="${di.pollingInterval}" min="1" style="width:70px;">
</label>

<label>
  <select name="digitalIntervalUnit">
    <option value="Sec" ${di.pollingIntervalUnit === "Sec" ? "selected" : ""}>Sec</option>
    <option value="Min" ${di.pollingIntervalUnit === "Min" ? "selected" : ""}>Min</option>
    <option value="Hour" ${di.pollingIntervalUnit === "Hour" ? "selected" : ""}>Hour</option>
  </select>
</label>

<br><br>

<table class="channel-table">
<tr><th>Channel</th><th>Name</th><th>Pin</th><th>Enable</th><th></th></tr>
${di.channels.map((ch, i) => `
<tr>
  <td>DI ${i + 1}</td>
  <td><input name="di_name_${i}" value="${ch.name}"></td>
  <td><input name="di_pin_${i}" value="${ch.pin}" placeholder="GPIO"></td>
  <td><input type="checkbox" name="di_enable_${i}" ${ch.enabled ? "checked" : ""}></td>
  <td><button type="button" onclick="removeDI(${i})">✕</button></td>
</tr>
`).join("")}
</table>

<button type="button" onclick="addDI()">+ Add Digital I/O</button>

<br><br>

<h3>Digital Output</h3>

<table class="channel-table">
<tr><th>Channel</th><th>Name</th><th>Pin</th><th>State</th><th></th></tr>
${doo.channels.map((ch, i) => `
<tr>
  <td>DO ${i + 1}</td>
  <td><input name="do_name_${i}" value="${ch.name}"></td>
  <td><input name="do_pin_${i}" value="${ch.pin}" placeholder="GPIO"></td>
  <td>
    <select name="do_state_${i}">
      <option value="0" ${ch.state === 0 ? "selected" : ""}>LOW</option>
      <option value="1" ${ch.state === 1 ? "selected" : ""}>HIGH</option>
    </select>
  </td>
  <td><button type="button" onclick="removeDO(${i})">✕</button></td>
</tr>
`).join("")}
</table>

<button type="button" onclick="addDO()">+ Add Digital Output</button>



<h3 style="margin-top:20px;">Relay Output</h3>

<table class="channel-table">
<tr>
<th>Relay</th>
<th>Name</th>
<th>Pin</th>
<th>Mode</th>
<th>Enable</th>
<th></th>
</tr>

${io.digitalRelay.channels.map((ch, i) => `
<tr>
<td>R${i + 1}</td>

<td>
  <input name="ro_name_${i}" value="${ch.name}">
</td>

<td>
  <input name="ro_pin_${i}" value="${ch.pin}" placeholder="GPIO / Relay Addr">
</td>

<td>
  <select name="ro_mode_${i}">
    <option value="NO" ${ch.mode === "NO" ? "selected" : ""}>Normally Open</option>
    <option value="NC" ${ch.mode === "NC" ? "selected" : ""}>Normally Closed</option>
  </select>
</td>

<td>
  <input type="checkbox" name="ro_enable_${i}" ${ch.enabled ? "checked" : ""}>
</td>

<td>
  <button type="button" onclick="removeRelay(${i})">✕</button>
</td>
</tr>
`).join("")}
</table>

<button type="button" onclick="addRelay()">+ Add Relay</button>


${renderDBSettings("digital", di.db)}

<br>
<button class="button-primary" type="submit">Save</button>

</form>`;
}

if (subTab === "Modbus RTU") {
  mount.innerHTML = `<div id="modbus-rtu-root"></div>`;
  renderModBusRTUPanel("Devices", null, null, "modbus-rtu-root");
}

if (subTab === "Modbus TCP") {
  mount.innerHTML = `<div id="modbus-tcp-root"></div>`;
  renderModbusTcpPanel("modbus-tcp-root");
}

  /* ================= TAB CLICK (FIXED) ================= */
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.onclick = () => {
      if (btn.classList.contains("active")) return;
      

      renderIOSettingsPanel(btn.dataset.tab);
    };
  });
  

  /* ================= SETTINGS SAVE HANDLER ================= */
  if (subTab === "Settings") {
    const f = document.getElementById("io-settings-form");
    if (f) {
      f.onsubmit = (e) => {
        e.preventDefault();

        config.ioSettings.settings ??= {};

        config.ioSettings.settings.modbus = f.modbus.checked;
        config.ioSettings.settings.modbusTCP = f.modbusTCP.checked;
        config.ioSettings.settings.analog = f.analog.checked;
        config.ioSettings.settings.digitalInput = f.digitalInput.checked;

        saveConfig();
      };
    }
  }

  if (subTab === "Analog I/O") {
    const a = config.ioSettings.analog;   // ✅ DEFINE IT HERE
    const f = document.getElementById("analog-form");

    f.onsubmit = (e) => {
      e.preventDefault();
      const fd = new FormData(f);

      a.pollingInterval = parseInt(fd.get("pollingInterval"), 10);
      a.pollingIntervalUnit = fd.get("intervalUnit");

      a.channels = [];
      config.ioSettings.analogOutput.channels = [];

for (let i = 0; fd.get(`ai_name_${i}`) !== null; i++) {
  const min = fd.get(`ai_min_${i}`);
  const max = fd.get(`ai_max_${i}`);
  const mode = fd.get(`ai_mode_${i}`)
  let symbol = ""
  if (mode.includes("mA")) symbol = "mA";
  if (mode.includes("V")) symbol = "V";
  
  let range = "";
  if (min !== "" && max !== "") {
    if (Number(min) >= Number(max)) {
      alert(`AI ${i + 1}: Process Min must be less than Process Max`);
      return;
    }
    range = `${min}:${max}${symbol}`;
  }

  a.channels.push({
    name: fd.get(`ai_name_${i}`).trim() || `AI${i + 1}`,
    enabled: fd.get(`ai_enable_${i}`) === "on",
    mode: mode,
    range,          // 🔥 SAME FIELD AS BEFORE
    address: fd.get(`ai_address_${i}`) || "",
  });


for (let i = 0; fd.get(`ao_name_${i}`) !== null; i++) {
  const min = fd.get(`ao_min_${i}`);
  const max = fd.get(`ao_max_${i}`);
  const mode = fd.get(`ao_mode_${i}`);

  let symbol = mode.includes("mA") ? "mA" : "V";
  let range = "";

  if (min !== "" && max !== "") {
    if (Number(min) >= Number(max)) {
      alert(`AO ${i + 1}: Process Min must be less than Process Max`);
      return;
    }
    range = `${min}:${max}${symbol}`;
  }

  config.ioSettings.analogOutput.channels.push({
    name: fd.get(`ao_name_${i}`) || `AO${i + 1}`,
    enabled: fd.get(`ao_enable_${i}`) === "on",
    mode,
    range,
    address: fd.get(`ao_address_${i}`) || ""
  });
}

}


      a.db.upload_local = fd.get("analog_upload_local") === "on";
      a.db.upload_cloud = fd.get("analog_upload_cloud") === "on";
      a.db.db_name = fd.get("analog_db_name") || "";
      a.db.table_name = fd.get("analog_table_name") || "";

      saveConfig();
    };

    // ✅ SAFE: a is in scope here
    requestAnimationFrame(() => {
      a.channels.forEach((ch, i) => {
        updateAnalogRange(i, ch.range);
      });
    });
  }


  if (subTab === "Digital I/O") {
    const f = document.getElementById("digital-io-form");

    f.onsubmit = (e) => {
      e.preventDefault();
      const fd = new FormData(f);

      const di = config.ioSettings.digitalInput;
      const doo = config.ioSettings.digitalOutput;

      di.pollingInterval = parseInt(fd.get("digitalPollingInterval"), 10);
      di.pollingIntervalUnit = fd.get("digitalIntervalUnit");

      di.channels = [];
      doo.channels = [];

      for (let i = 0; fd.get(`di_name_${i}`) !== null; i++) {
        di.channels.push({
          name: fd.get(`di_name_${i}`).trim() || `DI${i + 1}`,
          enabled: fd.get(`di_enable_${i}`) === "on",
          pin: fd.get(`di_pin_${i}`) || "",
        });
      }

      for (let i = 0; fd.get(`do_name_${i}`) !== null; i++) {
        doo.channels.push({
          name: fd.get(`do_name_${i}`).trim() || `DO${i + 1}`,
          state: parseInt(fd.get(`do_state_${i}`), 10),
          pin: fd.get(`do_pin_${i}`) || "",
        });
      }

      di.db.upload_local = fd.get("digital_upload_local") === "on";
      di.db.upload_cloud = fd.get("digital_upload_cloud") === "on";
      di.db.db_name = fd.get("digital_db_name") || "";
      di.db.table_name = fd.get("digital_table_name") || "";

      const ro = config.ioSettings.digitalRelay;
ro.channels = [];

for (let i = 0; fd.get(`ro_name_${i}`) !== null; i++) {
  ro.channels.push({
    name: fd.get(`ro_name_${i}`).trim() || `R${i + 1}`,
    pin: fd.get(`ro_pin_${i}`) || "",
    mode: fd.get(`ro_mode_${i}`) || "NO",
    enabled: fd.get(`ro_enable_${i}`) === "on"
  });
}

      saveConfig();
    };
  }

  

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
  if (!config.ModbusRTU.Devices)
    config.ModbusRTU.Devices = { brands: {}, order: [], globalPresets: {} };
  if (!config.ModbusRTU.Devices.globalPresets)
    config.ModbusRTU.Devices.globalPresets = {};
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
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },
      {
        name: "Current L2",
        start: 3002,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },
      {
        name: "Current L3",
        start: 3004,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },

      // L-L Voltages
      {
        name: "Voltage L1-L2",
        start: 3020,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L2-L3",
        start: 3022,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L3-L1",
        start: 3024,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },

      // L-N Voltages
      {
        name: "Voltage L1-N",
        start: 3028,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L2-N",
        start: 3030,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },
      {
        name: "Voltage L3-N",
        start: 3032,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
        length: 2,
        enabled: true,
      },

      // Power Factor
      {
        name: "Power Factor Total",
        start: 3084,
        offset: 0,
        type: "Input Register",
        conversion: "Float: Big Endian",
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
      })[m],
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

function flashTick(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = "inline";
  setTimeout(() => (el.style.display = "none"), 1200);
}



async function renderModBusRTUPanel(
  activeTopTab = "Devices",
  brandKey = null,
  activeSlaveId = null,
  containerId = "main-panel" 
) {
  
  ensureBase();
  if (!renderModBusRTUPanel._roleFetched) {
    await fetchRole();
    renderModBusRTUPanel._roleFetched = true;
  }

  const tabs = [
    // { name: "Settings", label: "Settings" },
    { name: "Devices", label: "Devices" },
    // {name:"PLC", label:"PLC"},
    // {name:"Transmitter", label:"Transmitter"}
  ];
  const topTabsHtml = `
          <div class="tab-list" role="tablist" aria-label="Main sections">
            ${tabs
      .map(
        (t) => `
              <button role="tab" class="tab-btn" aria-selected="${t.name === activeTopTab ? "true" : "false"
          }"
                      tabindex="${t.name === activeTopTab ? "0" : "-1"
          }" data-top="${t.name}">
                ${t.label}
              </button>`,
      )
      .join("")}
          </div>
        `;

  if (activeTopTab === "Settings") {
    renderSettings(topTabsHtml, containerId);
    wireTopTabs(activeTopTab, brandKey, activeSlaveId, containerId);
    wireTabsActions();
    return;
  }
  if (activeTopTab === "Devices") {
    renderModbusDevices(
      topTabsHtml,
      brandKey,
      activeSlaveId,
      containerId   // 🔥 REQUIRED
    );
    wireTopTabs(activeTopTab, brandKey, activeSlaveId, containerId);
    wireTabsActions();
    return;
  }
  const panel = document.getElementById(containerId);
  panel.innerHTML = `
          <div class="panel-header">${activeTopTab}</div>
          ${topTabsHtml}
          <div class="tab-content" role="tabpanel" style="margin-top:8px;">Coming soon…</div>
        `;
  wireTopTabs(activeTopTab, brandKey, activeSlaveId, containerId);
  wireTabsActions();
}

function wireTopTabs(activeTopTab, brandKey, activeSlaveId, containerId) {
  document.querySelectorAll('[role="tab"][data-top]').forEach((btn) => {
    btn.onclick = () =>
      renderModBusRTUPanel(
        btn.getAttribute("data-top"),
        brandKey,
        activeSlaveId,
        containerId          // 🔥 KEEP IT
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
        "Custom tab removal is ephemeral in this demo. Reload resets custom tabs.",
      );
    }
  };
}

// Settings
function renderSettings(topTabsHtml, containerId = "main-panel") {
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
                    <option value="${b}" ${String(s.baudRate) === b ? "selected" : ""
          }>${b}</option>`,
      )
      .join("")}
                </select>
              </label>
              <label>Parity:
                <select name="parity">
                  ${["Even", "Odd", "None"]
      .map(
        (p) => `
                    <option value="${p}" ${s.parity === p ? "selected" : ""
          }>${p}</option>`,
      )
      .join("")}
                </select>
              </label>
              <label>Data Bits:
                <select name="dataBits">
                  ${[5, 6, 7, 8]
      .map(
        (d) =>
          `<option value="${d}" ${Number(s.dataBits) === d ? "selected" : ""
          }>${d}</option>`,
      )
      .join("")}
                </select>
              </label>
              <label>Stop Bits:
                <select name="stopBits" id="stopBits">
                  ${[1, 2]
      .map(
        (v) =>
          `<option value="${v}" ${Number(s.stopBits) === v ? "selected" : ""
          }>${v}</option>`,
      )
      .join("")}
                </select>
              </label>
              <div class="toolbar">
                <button type="submit" class="button-primary">Save</button>
                <span class="checkmark" id="rtu-settings-tick" style="display:none;">&#10004;</span>
              </div>
            </form>
          </div>
        `;
        const panel = document.getElementById(containerId);
        if (!panel) {
          console.error("RTU settings container missing:", containerId);
          return;
        }
        panel.innerHTML = html;
        


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

function getAllUsedSlaveIds() {
  const ids = new Set();
  const brands = config.ModbusRTU?.Devices?.brands || {};
  Object.values(brands).forEach(b => {
    (b.slaves || []).forEach(s => ids.add(Number(s.id)));
  });
  return ids;
}

function suggestNextSlaveId() {
  const used = getAllUsedSlaveIds();
  for (let i = 1; i <= 247; i++) {
    if (!used.has(i)) return i;
  }
  alert("No free slave IDs (1–247)");
  return null;
}

function normalizeSlave(s) {
  return {
    id: Number(s.id),
    enabled: s.enabled ?? true,

    // 🔥 COMMUNICATION
    rs485_port: s.rs485_port ?? "/dev/ttyAMA5",
    baudRate: s.baudRate ?? 9600,
    parity: s.parity ?? "Even",
    dataBits: s.dataBits ?? 8,
    stopBits: s.stopBits ?? 1,

    // 🔥 BEHAVIOR
    pollingInterval: s.pollingInterval ?? 5,
    pollingIntervalUnit: s.pollingIntervalUnit ?? "Sec",
    use_usb: s.use_usb ?? false,

    // 🔥 DB
    upload_local: s.upload_local ?? true,
    upload_cloud: s.upload_cloud ?? false,
    db_name: s.db_name ?? "",
    table_name: s.table_name ?? "",
  };
}

function normalizeConversion(v) {
  if (!v) return "Raw Hex";
  if (v.startsWith("Float")) return "Float: Big Endian";
  if (v.startsWith("Integer")) return "Integer";
  if (v.startsWith("Double")) return "Double";
  return "Raw Hex";
}

function readDBSettings(prefix) {
  const q = (n) => document.querySelector(`[name="${prefix}_${n}"]`);
  return {
    upload_local: q("upload_local")?.checked ?? true,
    upload_cloud: q("upload_cloud")?.checked ?? false,
    db_name: q("db_name")?.value.trim() || "",
    table_name: q("table_name")?.value.trim() || "",
  };
}

function normalizeRegisterRow(r) {
  return {
    name: r.name ?? "",
    start: r.start ?? "",
    offset: r.offset ?? 0,
    type: r.type ?? "Input Register",
    conversion: r.conversion ?? "Float: Big Endian",
    sql_type: r.sql_type ?? "FLOAT",
    length: r.length ?? 2,
    multiply: r.multiply ?? 1,
    divide: r.divide ?? 1,
    process_min: r.process_min ?? "",
    process_max: r.process_max ?? "",
    sensor_type: r.sensor_type ?? "",
    eng_symbol: r.eng_symbol ?? "",

    eng_unit: r.eng_unit ?? "4-20mA",   // ✅ NEW
    enabled: !!r.enabled,
  };
}

function slaveSettingsModal(slave) {
  const rs485Ports = config.adminSettings?.rs485Ports || [];

  return `
<div id="slave-settings-modal"
     class="modal"
     aria-modal="true"
     role="dialog"
     style="display:flex;">
  <div class="modal-content" style="min-width:420px;">
    <h3>Slave ${slave.id} – RTU Settings</h3>

    <fieldset>
      <legend><b>Communication</b></legend>

      <label>RS485 Port:
        <select id="slave-rs485-port" style="width:260px;">
          ${rs485Ports.length === 0
      ? `<option disabled selected>No RS485 ports configured</option>`
      : rs485Ports.map(p => `
                <option value="${p.port}"
                  ${slave.rs485_port === p.port ? "selected" : ""}>
                  ${p.name}
                </option>
              `).join("")}
        </select>
      </label>

      <label>Baud Rate:
        <select id="slave-baud">
          ${["300", "600", "1200", "2400", "4800", "9600", "19200", "38400", "57600", "115200", "230400"]
      .map(b =>
        `<option value="${b}" ${Number(slave.baudRate) == b ? "selected" : ""}>${b}</option>`
      ).join("")}
        </select>
      </label>

      <label>Parity:
        <select id="slave-parity">
          ${["Even", "Odd", "None"]
      .map(p =>
        `<option value="${p}" ${slave.parity === p ? "selected" : ""}>${p}</option>`
      ).join("")}
        </select>
      </label>

      <label>Data Bits:
        <select id="slave-databits">
          ${[5, 6, 7, 8]
      .map(d =>
        `<option value="${d}" ${slave.dataBits === d ? "selected" : ""}>${d}</option>`
      ).join("")}
        </select>
      </label>

      <label>Stop Bits:
        <select id="slave-stopbits">
          ${[1, 2]
      .map(s =>
        `<option value="${s}" ${slave.stopBits === s ? "selected" : ""}>${s}</option>`
      ).join("")}
        </select>
      </label>
    </fieldset>

    <div class="toolbar" style="margin-top:14px;">
      <button id="slave-settings-save" class="button-primary">Save</button>
      <button id="slave-settings-cancel" class="button">Cancel</button>
    </div>
  </div>
</div>`;
}


function openSlaveSettings(brandKey, slaveId) {
  const existing = document.getElementById("slave-settings-modal");
  if (existing) existing.remove();

  const brand = config.ModbusRTU.Devices.brands[brandKey];
  const slave = brand.slaves.find(s => String(s.id) === String(slaveId));
  if (!slave) return;

  const wrapper = document.createElement("div");
  wrapper.innerHTML = slaveSettingsModal(slave);
  document.body.appendChild(wrapper.firstElementChild);

  const modal = document.getElementById("slave-settings-modal");

  document.getElementById("slave-settings-cancel").onclick = () => {
    modal.remove();
  };

  document.getElementById("slave-settings-save").onclick = () => {
    slave.rs485_port =
      document.getElementById("slave-rs485-port").value.trim();

    slave.baudRate =
      Number(document.getElementById("slave-baud").value);

    slave.parity =
      document.getElementById("slave-parity").value;

    slave.dataBits =
      Number(document.getElementById("slave-databits").value);

    slave.stopBits =
      Number(document.getElementById("slave-stopbits").value);

    slave.pollingInterval =
      Number(document.getElementById("slave-polling-interval").value) || 1;


    saveConfig();
    modal.remove();
    renderModBusRTUPanel("Devices", brandKey, slaveId);
  };

}

window.openSlaveSettings = openSlaveSettings; // ✅ REQUIRED

// Energy Meter (brands as tabs; slaves as tabs; presets; manual save)
function renderModbusDevices(
  topTabsHtml,
  currentBrandKey = null,
  currentSlaveId = null,
  containerId
) {
  if (!containerId) {
    throw new Error("renderModbusDevices called without containerId");
  }
  const em = config.ModbusRTU.Devices;
  if (em.order.length === 0) {
    const k = "schneider_em6436h";
    if (!em.brands[k])
      em.brands[k] = {
        label: builtInSchemas[k]?.label || "Schneider EM6436H",
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
                  <button role="tab" class="brand-tab" aria-selected="${sel ? "true" : "false"
          }" tabindex="${sel ? "0" : "-1"
          }" data-brand="${k}">${lbl}</button>
                  <button class="chip-del em-brand-del" data-brand="${k}" title="Remove Brand">×</button>
                </span>
              `;
      })
      .join("")}
            <button id="em-add-brand" class="button-primary" style="margin-left:6px;">＋ Device</button>
          </div>
        `;

  const slaves = (brand.slaves || []).map(normalizeSlave);
  brand.slaves = slaves; // 🔥 persist normalization
  const slaveId = currentSlaveId || (slaves[0]?.id ?? null);

  const slaveTabs = `
          <div style="margin-top:12px;">
            <div class="tab-list" role="tablist" aria-label="Slaves">
              ${slaves
      .map((s) => {
        const sel = String(s.id) === String(slaveId);
        const disabledStyle = s.enabled === false
          ? "opacity:0.5; text-decoration:line-through;"
          : "";

        return `
                  <span class="slave-pill" data-slave="${s.id
          }" style="display:inline-flex;align-items:center;">
                    <button role="tab" class="slave-tab" aria-selected="${sel ? "true" : "false"
          }" tabindex="${sel ? "0" : "-1"}" data-slave="${s.id}">
                      <span class="slave-label" data-slave="${s.id}">${s.id
          }</span>
                      <input class="slave-input" type="number" min="1" max="247" data-slave="${s.id
          }" value="${s.id
          }" style="display:none;width:64px;margin-left:4px;">
                    </button>
                      <button class="chip" onclick="openSlaveSettings('${brandKey}', '${s.id}')">⚙️</button>
                    <button class="chip-del em-slave-del" data-slave="${s.id
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
    slaveId == null
      ? []
      : (brand.registersBySlave?.[regKey] || []).map(normalizeRegisterRow);

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
    ([pid, p]) => ({ id: `global:${pid}`, name: `Global: ${p.name || pid}` }),
  );
  const presetToolbar = `
          <select id="em-preset-select">
            <option value="">Load preset…</option>
            ${builtIn
      .map((p) => `<option value="${p.id}">${p.name}</option>`)
      .join("")}
            ${brandPresets.length
      ? `<optgroup label="Brand presets">${brandPresets
        .map((p) => `<option value="${p.id}">${p.name}</option>`)
        .join("")}</optgroup>`
      : ""
    }
            ${globalPresets.length
      ? `<optgroup label="Global presets">${globalPresets
        .map((p) => `<option value="${p.id}">${p.name}</option>`)
        .join("")}</optgroup>`
      : ""
    }
          </select>
          <button id="em-load-preset-btn" class="button">Load</button>
          <select id="em-preset-scope" class="button"  style="width:100%"><option value="">Select to save the preset for Device/Global</option><option value="brand">Device</option><option value="global">Global</option></select>
          <button id="em-save-preset-btn" class="button">Save as Preset</button>
        `;

  const currentSlaveObj =
    slaveId != null
      ? brand.slaves.find((s) => String(s.id) === String(slaveId))
      : null;

  const slaveSettings = currentSlaveObj
    ? `
  <div style="margin-top:10px;padding:8px;border:1px solid #ccc;">
    <b>Slave ${slaveId} Settings</b>

    <!-- 🔥 POLLING -->
    <div style="display:flex;gap:12px;align-items:center;margin-top:8px;">
      <label>
        Polling Interval:
        <input type="number"
          id="slave-polling-interval"
          min="1"
          value="${currentSlaveObj.pollingInterval}"
          style="width:70px;">
      </label>

  

      <select id="slave-polling-unit">
        ${["Sec", "Min", "Hour"]
      .map(
        (u) =>
          `<option value="${u}" ${u === currentSlaveObj.pollingIntervalUnit ? "selected" : ""
          }>${u}</option>`,
      )
      .join("")}
      </select>

<div style="margin-top:8px; display:flex; gap:20px;">
  <label style="display:flex; align-items:center; gap:6px;">
    <input type="checkbox" id="slave-enabled"
      ${currentSlaveObj.enabled ? "checked" : ""}>
    <b>Slave Enabled</b>
  </label>

  <label style="display:flex; align-items:center; gap:6px;">
    <input type="checkbox" id="slave-use-usb"
      ${currentSlaveObj.use_usb ? "checked" : ""}>
    <b>Use USB</b>
  </label>
</div>


    </div>


    ${renderDBSettings("slave", currentSlaveObj, { hideDbName: true })}

    <div style="margin-top:12px;" class="toolbar">
      <button id="em-save" class="button-primary">Save</button>
      <span class="checkmark" id="em-tick" style="display:none;">&#10004;</span>
    </div>
  </div>
  `
    : "";

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
            <th >Type of Register</th>
                <th style="display:none">Conversion</th>
                <th>Type of Sensor</th>
<th>Engineering Symbol</th>

                <th>Electrical Range</th>
                <th>Data Type</th>
                <th>Length</th>
                <th>Multiply</th>
<th>Divide</th>
                <th>Process Range</th>
                <th>Enabled</th>
                <th>Remove</th>
              </tr>
              ${rows.map((r, i) => rowHtml(r, i)).join("")}
            </table>

          </div>
        `;

  const html = `
          <div class="panel-header" style="display:none">Modbus RTU Devices</div>
          ${topTabsHtml}
          <div class="tab-content" role="tabpanel" style="margin-top:8px;">
            ${brandTabs}
            ${slaveTabs}
            ${slaveSettings}
            ${regsTable}
          </div>
          ${brandAddModal()}
          ${presetNameModal()}
        `;
  const panel = document.getElementById(containerId);
  panel.innerHTML = html;

  // Brand handlers
  document.querySelectorAll('.brand-tab[role="tab"]').forEach((b) => {
    b.onclick = () =>
      renderModbusDevices(topTabsHtml, b.getAttribute("data-brand"), null, containerId);
  });
  document.querySelectorAll(".em-brand-del").forEach((b) => {
    b.onclick = () => {
      const k = b.getAttribute("data-brand");
      if (
        !confirm(
          `Remove brand "${em.brands[k]?.label || k
          }"? This deletes its slaves and registers.`,
        )
      )
        return;
      if (em.brands[k]) delete em.brands[k];
      em.order = em.order.filter((x) => x !== k);

      // 🔥 DELETE BRAND ALARMS
      deleteModbusBrandAlarms(k);
      saveConfig();
      const next = em.order[0] || null;
      renderModBusRTUPanel("Devices", next, null,containerId);
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
      renderModBusRTUPanel("Devices", key, null, containerId);
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
        renderModbusDevices(topTabsHtml, brandKey, id, containerId);
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

        const used = getAllUsedSlaveIds();
        used.delete(Number(oldId)); // allow replacing itself

        if (used.has(n)) {
          alert(`Slave ID ${n} already exists in another device.`);
          cancel();
          return;
        }


        // 🔥 PRESERVE ALL SLAVE FIELDS
        brand.slaves = brand.slaves.map((s) =>
          String(s.id) === oldId ? { ...s, id: n } : s,
        );

        // 🔥 MOVE REGISTERS KEY
        brand.registersBySlave[String(n)] = brand.registersBySlave[oldId] || [];
        delete brand.registersBySlave[oldId];
        migrateModbusSlaveId(brandKey, oldId, String(n));

        saveConfig();
        renderModbusDevices(topTabsHtml, brandKey, String(n), containerId);
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
      renderModbusDevices(topTabsHtml, brandKey, next, containerId);
    };
  });
  document.getElementById("em-add-slave")?.addEventListener("click", () => {
    const nextId = suggestNextSlaveId();
    brand.slaves = brand.slaves || [];
    brand.slaves.push(
      normalizeSlave({ id: nextId })
    );

    brand.registersBySlave = brand.registersBySlave || {};
    brand.registersBySlave[String(nextId)] =
      brand.registersBySlave[String(nextId)] || [];
    saveConfig();
    renderModbusDevices(topTabsHtml, brandKey, String(nextId), containerId);
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
        ["start", "offset", "length", "multiply", "divide"].includes(field) &&
        e.target.type !== "checkbox"
      )
        v = parseInt(v, 10);
      workingRows[idx] = workingRows[idx] || {};
      workingRows[idx][field] = v;
      if (field === "sensor_type") {
        const cfg = getEngineeringConfig()
          .find(e => e.type === v);

        workingRows[idx].eng_symbol =
          cfg && cfg.symbols.length ? cfg.symbols[0] : "";

        rerenderRegsTable(workingRows);
      }

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
        sensor_type: "",
        eng_unit: "",
        conversion: "Float: Big Endian",
        sql_type: "FLOAT", // 🔥 NEW
        length: 2,
        multiply: 1,
        divide: 1,
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
        workingRows = (builtInSchemas[key]?.rows || []).map(normalizeRegisterRow);

      } else if (sel.startsWith("brand:")) {
        const pid = sel.split(":")[1];
        workingRows = ((brand.presets || {})[pid]?.rows || []).map(normalizeRegisterRow);
      } else if (sel.startsWith("global:")) {
        const pid = sel.split(":")[1];
        workingRows = ((em.globalPresets || {})[pid]?.rows || []).map(normalizeRegisterRow);
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
        if (currentSlaveObj) {
          Object.assign(currentSlaveObj, readDBSettings("slave"));
          // 🔥 SAVE POLLING
          currentSlaveObj.enabled =
            document.getElementById("slave-enabled")?.checked ?? true;
          currentSlaveObj.use_usb =
            document.getElementById("slave-use-usb")?.checked ?? false;


          currentSlaveObj.pollingInterval =
            parseInt(
              document.getElementById("slave-polling-interval")?.value,
              10,
            ) || 1;

          currentSlaveObj.pollingIntervalUnit =
            document.getElementById("slave-polling-unit")?.value || "Sec";
        }

        saveConfig();
        renderModbusDevices(topTabsHtml, brandKey, slaveId, containerId);
      });
    });

    const saveBtn = document.getElementById("em-save");
    if (saveBtn) {
      saveBtn.onclick = () => {
        brand.registersBySlave = brand.registersBySlave || {};
        brand.registersBySlave[regKey] =
          deepClone(workingRows.map(normalizeRegisterRow));


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
          if (r.sensor_type && !r.eng_symbol) {
            alert(`Row ${i + 1}: Engineering symbol is required for sensor type "${r.sensor_type}"`);
            return;
          }
        }

        if (currentSlaveObj) {
          Object.assign(currentSlaveObj, readDBSettings("slave"));

          currentSlaveObj.enabled =
            document.getElementById("slave-enabled")?.checked ?? true;
          currentSlaveObj.use_usb =
            document.getElementById("slave-use-usb")?.checked ?? false;


          currentSlaveObj.pollingInterval =
            parseInt(
              document.getElementById("slave-polling-interval")?.value,
              10,
            ) || 1;

          currentSlaveObj.pollingIntervalUnit =
            document.getElementById("slave-polling-unit")?.value || "Sec";
        }

        saveConfig();
        flashTick("em-tick");
      };
    }

  } else {
    const saveBtn = document.getElementById("em-save");
    if (saveBtn) {
      saveBtn.onclick = () => {
        brand.registersBySlave = brand.registersBySlave || {};
        brand.registersBySlave[regKey] =
          deepClone(workingRows.map(normalizeRegisterRow));


        for (let i = 0; i < workingRows.length; i++) {
          const r = workingRows[i];
          if (r.sensor_type && !r.eng_symbol) {
            alert(`Row ${i + 1}: Engineering symbol is required for sensor type "${r.sensor_type}"`);
            return;
          }
          if (
            r.process_min !== "" &&
            r.process_max !== "" &&
            Number(r.process_min) >= Number(r.process_max)
          ) {
            alert(`Row ${i + 1}: Process Min must be less than Process Max`);
            return;
          }
        }

        if (currentSlaveObj) {
          Object.assign(currentSlaveObj, readDBSettings("slave"));

          currentSlaveObj.enabled =
            document.getElementById("slave-enabled")?.checked ?? true;
          currentSlaveObj.use_usb =
            document.getElementById("slave-use-usb")?.checked ?? false;


          currentSlaveObj.pollingInterval =
            parseInt(
              document.getElementById("slave-polling-interval")?.value,
              10,
            ) || 1;

          currentSlaveObj.pollingIntervalUnit =
            document.getElementById("slave-polling-unit")?.value || "Sec";
        }

        saveConfig();
        flashTick("em-tick");
      };
    }

  }
}

function migrateModbusSlaveId(brandKey, oldId, newId) {
  const mod = config.alarmSettings?.Modbus;
  if (!mod?.[brandKey]?.slaves?.[oldId]) return;

  mod[brandKey].slaves[newId] = mod[brandKey].slaves[oldId];
  delete mod[brandKey].slaves[oldId];
}

function deleteModbusBrandAlarms(brandKey) {
  if (
    config.alarmSettings?.Modbus &&
    config.alarmSettings.Modbus[brandKey]
  ) {
    delete config.alarmSettings.Modbus[brandKey];
  }
}

function getEngineeringConfig() {
  return config.adminSettings.engineeringUnits || [];
}


function rowHtml(r, i) {
  return `
          <tr>
            <td>${i + 1}</td>
            <td><input type="text" value="${esc(
    r.name,
  )}" data-field="name" data-index="${i}" style="width:180px;"></td>
            <td><input type="number" value="${numOrEmpty(
    r.start,
  )}" data-field="start" data-index="${i}" style="width:90px;"></td>
            <td><input type="number" value="${numOrEmpty(
    r.offset,
    0,
  )}" data-field="offset" data-index="${i}" style="width:70px;"></td>
            <td >
              <select data-field="type" data-index="${i}">
                              <option value="Coil" ${r.type === "Coil" ? "selected" : ""
    }>Coil</option>
                    <option value="Discrete Input" ${r.type === "Discrete Input" ? "selected" : ""
    }>Discrete Input</option>
                <option value="Holding Register" ${r.type === "Holding Register" ? "selected" : ""
    }>Holding Register</option>
                <option value="Input Register" ${r.type === "Input Register" ? "selected" : ""
    }>Input Register</option>
              </select>
            </td>
            <td style="display:none">
              <select data-field="conversion" data-index="${i} ">
                <option value="Raw Hex" ${r.conversion === "Raw Hex" ? "selected" : ""
    }>Raw Hex</option>
                <option value="Integer" ${r.conversion === "Integer" ? "selected" : ""
    }>Integer</option>
                <option value="Double" ${r.conversion === "Double" ? "selected" : ""
    }>Double</option>
                <option value="Float: Big Endian" ${r.conversion === "Float: Big Endian" ? "selected" : ""
    }>Float: Big Endian</option>
              </select>
            </td>
<td>
  <select data-field="sensor_type" data-index="${i}">
    <option value="">— Select —</option>
    ${getEngineeringConfig().map(e => `
      <option value="${e.type}" ${r.sensor_type === e.type ? "selected" : ""}>
        ${e.type}
      </option>
    `).join("")}
  </select>
</td>


<td>
  ${(() => {
      const cfg = getEngineeringConfig()
        .find(e => e.type === r.sensor_type);

      if (!cfg || !cfg.symbols.length) return "";

      return `
        <select data-field="eng_symbol" data-index="${i}">
          ${cfg.symbols.map(sym => `
            <option value="${sym}" ${r.eng_symbol === sym ? "selected" : ""}>
              ${sym}
            </option>
          `).join("")}
        </select>
      `;
    })()
    }
</td>


            <td>
  <select data-field="eng_unit" data-index="${i}">
    <option value="4-20mA" ${r.eng_unit === "4-20mA" ? "selected" : ""}>4–20 mA</option>
    <option value="0-20mA" ${r.eng_unit === "0-20mA" ? "selected" : ""}>0–20 mA</option>
    <option value="0-10V"  ${r.eng_unit === "0-10V" ? "selected" : ""}>0–10 V</option>
        <option value="0-5V"  ${r.eng_unit === "0-5V" ? "selected" : ""}>0–5 V</option>
                <option value="1-5V"  ${r.eng_unit === "1-5V" ? "selected" : ""}>1–5 V</option>
                                <option value="none"  ${r.eng_unit === "none" ? "selected" : ""}>Modbus RTU</option>



  </select>
</td>

            <td>
    <select data-field="sql_type" data-index="${i}">
      <option value="FLOAT" ${r.sql_type === "FLOAT" ? "selected" : ""}>FLOAT</option>
      <option value="INT" ${r.sql_type === "INT" ? "selected" : ""}>INT</option>
      <option value="BIGINT" ${r.sql_type === "BIGINT" ? "selected" : ""}>BIGINT</option>
      <option value="VARCHAR" ${r.sql_type === "VARCHAR" ? "selected" : ""}>VARCHAR</option>
      <option value="BOOLEAN" ${r.sql_type === "BOOLEAN" ? "selected" : ""}>BOOLEAN</option>
    </select>
  </td>

            <td><input type="number" value="${numOrEmpty(
      r.length,
      2,
    )}" data-field="length" data-index="${i}" style="width:70px;"></td>
            <td>
  <input type="number"
    step="any"
    value="${numOrEmpty(r.multiply, 1)}"
    data-field="multiply"
    data-index="${i}"
    style="width:70px;">
</td>

<td>
  <input type="number"
    step="any"
    value="${numOrEmpty(r.divide, 1)}"
    data-field="divide"
    data-index="${i}"
    style="width:70px;">
</td>

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

            <td><input type="checkbox" ${r.enabled ? "checked" : ""
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
            <th>Type of Register</th>
                <th style="display:none">Conversion</th>
                <th>Type of Sensor</th>
<th>Engineering Symbol</th>

            <th>Electrical Range</th>
            <th>Data Type</th>
            <th>Length</th>
            <th>Multiply</th>
<th>Divide</th>

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
  } catch (e) { }
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
        ${plcConfigs.map((plc, index) => renderRtuPlcEntry(plc, index)).join("")}
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
            <button type="button" class="button toggle-plc-details" title="Toggle">${isExpanded ? "−" : "+"}</button>
            <button type="button" class="button remove-plc-entry" title="Remove">×</button>
          </div>
        </div>
        <div class="plc-details" style="${isExpanded ? "" : "display:none;"}">
          <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; margin:12px 0;">
            <label>PLC Type:
              <select class="plc-type-select">
                <option value="Other" ${isOther ? "selected" : ""}>Other</option>
                <option value="Siemens" ${plc.plcType === "Siemens" ? "selected" : ""}>Siemens</option>
                <option value="Allen Bradley" ${isAllenBradley ? "selected" : ""}>Allen Bradley</option>
              </select>
            </label>
            <label class="plc-driver-label" style="${isAllenBradley ? "" : "display:none;"}">
              Driver Type:
              <select class="plc-driver-select">
                <option value="logix" ${plc.PLC?.cred?.driver === "logix" ? "selected" : ""}>Logix</option>
                <option value="slc" ${plc.PLC?.cred?.driver === "slc" ? "selected" : ""}>SLC</option>
              </select>
            </label>
            <label class="plc-name-label" style="${isOther ? "display:none;" : ""}">
              Table Name:
              <input type="text" class="plc-name-input" value="${plc.PLC?.Database?.table_name || ""}" />
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
    `.plc-entry[data-index='${index}']`,
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
        <td><input type="text" data-write-field="content" value="${item.content || ""}" placeholder="Tag"></td>
        <td><input type="number" data-write-field="address" value="${item.address ?? 1}" min="0"></td>
        <td><input type="number" data-write-field="value_to_write" value="${item.value_to_write ?? 0}" step="1" min="0" max="1"></td>
        <td><button type="button" class="button remove-siemens-write-row">-</button></td>
      </tr>
    `,
    )
    .join("");

  return `
      <form id="siemens-form-${index}">
        <fieldset><legend>RTU Credentials</legend>
          <label>Serial Port: <input type="text" data-cred-field="port" value="${c.port}"></label>
          <label>Baudrate: <input type="number" data-cred-field="baudrate" value="${c.baudrate}" min="1200" max="115200" step="600"></label>
          <label>Parity:
            <select data-cred-field="parity">
              ${["N", "E", "O"].map((p) => `<option value="${p}" ${c.parity === p ? "selected" : ""}>${p}</option>`).join("")}
            </select>
          </label>
          <label>Stop Bits:
            <select data-cred-field="stopbits">
              ${[1, 2].map((s) => `<option value="${s}" ${c.stopbits == s ? "selected" : ""}>${s}</option>`).join("")}
            </select>
          </label>
          <label>Slave ID: <input type="number" data-cred-field="slave_id" value="${c.slave_id}" min="1" max="247"></label>
        </fieldset>

        <fieldset><legend>Read Items</legend>
          <table>
            <thead>
              <tr>
                <th>Data Type</th><th>Content</th><th>Address</th><th>Length</th><th>Datatype</th><th>Read</th><th>Write</th><th>Remove</th>
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
          <input type="number" data-freq-field="data_reading_freq(in secs)" value="${plcConfig["data_reading_freq(in secs)"] || 180}" step="0.1">
        </label>
      </form>
    `;
}

function renderRtuSiemensRow(item, rowIndex) {
  return `
      <tr data-row-index="${rowIndex}">
        <td>
          <select data-field="type">
            ${["holding_register", "input_register", "coil", "discrete_input"].map((t) => `<option value="${t}" ${item.type === t ? "selected" : ""}>${t}</option>`).join("")}
          </select>
        </td>
        <td><input type="text" data-field="content" value="${item.content || ""}" placeholder="Tag"></td>
        <td><input type="number" data-field="address" value="${item.address || 0}" placeholder="40001"></td>
        <td><input type="number" data-field="length" value="${item.length || 1}" min="1"></td>
        <td>
          <select data-field="datatype">
            ${["float", "int", "real", "bool"].map((d) => `<option value="${d}" ${item.datatype === d ? "selected" : ""}>${d.toUpperCase()}</option>`).join("")}
          </select>
        </td>
        <td><input type="checkbox" data-field="read" ${item.read !== false ? "checked" : ""}></td>
        <td><input type="checkbox" data-field="write" ${item.write ? "checked" : ""}></td>
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
        <td><input type="text" data-write-field="content" value="${item.content || ""}" placeholder="Tag"></td>
        <td><input type="number" data-write-field="address" value="${item.address ?? 1}" min="0"></td>
        <td><input type="number" data-write-field="value_to_write" value="${item.value_to_write ?? 0}" step="1" min="0" max="1"></td>
        <td><button type="button" class="button remove-siemens-write-row">-</button></td>
      </tr>
    `,
    )
    .join("");

  const addressChain = `
      <div class="address-value-chain" style="display:${isLogix ? "block" : "none"}; margin-top:16px;">
        <div class="soft" style="margin-bottom:8px;">Enter the tag path segments for Logix (e.g., Program:MainProgram → MyAOI → PV).</div>
        <div class="address-chain" style="display:flex; align-items:center; flex-wrap:wrap; gap:8px; padding:12px; border:1px solid #ddd; border-radius:6px; background:#fafafa;">
          ${(plcConfig.address_of_value || [""])
      .map(
        (value, nodeIndex) => `
            <div style="display:flex; align-items:center; gap:6px;">
              <input type="text" class="ab-path-node" data-node-index="${nodeIndex}" value="${value || ""}" placeholder="Segment ${nodeIndex + 1}" style="padding:6px; border:1px solid #ccc; border-radius:4px; font-family:monospace; width:160px;">
              <button type="button" class="button ab-remove-node" data-node-index="${nodeIndex}" style="display:${(plcConfig.address_of_value || []).length > 1 ? "inline-block" : "none"};">×</button>
              ${nodeIndex < (plcConfig.address_of_value || []).length - 1 ? '<span style="margin:0 4px;">→</span>' : ""}
            </div>
          `,
      )
      .join("")}
          <button type="button" class="button ab-add-node">＋</button>
        </div>
      </div>
    `;

  return `
      <form id="ab-form-${index}">
        <fieldset><legend>RTU Credentials</legend>
          <label>Serial Port: <input type="text" data-cred-field="port" value="${c.port || (navigator.platform && navigator.platform.startsWith("Win") ? "COM3" : "/dev/ttyUSB0")}"></label>
          <label>Baudrate: <input type="number" data-cred-field="baudrate" value="${c.baudrate || 9600}" min="1200" max="115200" step="600"></label>
          <label>Parity:
            <select data-cred-field="parity">
              ${["N", "E", "O"].map((p) => `<option value="${p}" ${c.parity === p ? "selected" : ""}>${p}</option>`).join("")}
            </select>
          </label>
          <label>Stop Bits:
            <select data-cred-field="stopbits">
              ${[1, 2].map((s) => `<option value="${s}" ${c.stopbits == s ? "selected" : ""}>${s}</option>`).join("")}
            </select>
          </label>
          <label>Slave ID: <input type="number" data-cred-field="slave_id" value="${c.slave_id ?? 1}" min="1" max="247"></label>
          <label>Driver:
            <select data-cred-field="driver" class="ab-driver-select">
              <option value="logix" ${c.driver === "logix" ? "selected" : ""}>Logix</option>
              <option value="slc" ${c.driver === "slc" ? "selected" : ""}>SLC</option>
            </select>
          </label>
        </fieldset>

        <fieldset><legend>Read Items</legend>
          <table>
            <thead>
              <tr>
                <th>Data Type</th><th>Content</th><th>Address</th><th>length</th><th>Datatype</th><th>Read</th><th>Write</th><th>Remove</th>
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
          <label>Name: <input class="tx-name" type="text" value="${t.name || ""}"></label>
          <label>Serial Port: <input class="tx-serial" type="text" value="${t.serialPort || "/dev/ttyUSB0"}"></label>
          <label>Baud: <input class="tx-baud" type="number" min="1200" step="1" value="${t.baudRate || 9600}"></label>
          <label>Data Bits:
            <select class="tx-dbits">
              <option value="7" ${t.dataBits === 7 ? "selected" : ""}>7</option>
              <option value="8" ${t.dataBits === 8 ? "selected" : ""}>8</option>
            </select>
          </label>
          <label>Parity:
            <select class="tx-parity">
              <option value="None" ${t.parity === "None" ? "selected" : ""}>None</option>
              <option value="Even" ${t.parity === "Even" ? "selected" : ""}>Even</option>
              <option value="Odd" ${t.parity === "Odd" ? "selected" : ""}>Odd</option>
            </select>
          </label>
          <label>Stop Bits:
            <select class="tx-sbits">
              <option value="1" ${t.stopBits === 1 ? "selected" : ""}>1</option>
              <option value="2" ${t.stopBits === 2 ? "selected" : ""}>2</option>
            </select>
          </label>
          <label>Unit ID: <input class="tx-unit" type="number" min="1" max="247" value="${t.unitId || 1}"></label>
          <label>Interval (s): <input class="tx-int" type="number" min="1" value="${t.interval || 5}"></label>
          <label>Function:
            <select class="tx-fc">
              <option value="3" ${t.functionCode === 3 ? "selected" : ""}>03 - Read Holding</option>
              <option value="4" ${t.functionCode === 4 ? "selected" : ""}>04 - Read Input</option>
            </select>
          </label>
        </div>

        <div style="margin-top:12px;">
          <div class="split-toolbar">
            <span class="soft">Registers</span>
            <button class="button tx-reg-add">＋ Add Register</button>
          </div>
          <div class="tx-reg-list">
            ${regs || `<div class="soft" style="margin-top:6px;">No registers yet.</div>`}
          </div>
        </div>
      </div>
    `;
}

function regRow(r, ti, ri) {
  return `
      <div class="row" data-tx="${ti}" data-reg="${ri}" style="display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; align-items:center;">
        <label style="min-width:180px;">Name: <input class="reg-name" type="text" value="${r.name || ""}"></label>
        <label>Address: <input class="reg-addr" type="number" min="0" value="${Number.isFinite(r.address) ? r.address : 0}"></label>
        <label>Count: <input class="reg-count" type="number" min="1" value="${r.count || 1}"></label>
        <label>Datatype:
          <select class="reg-type">
            <option value="int16" ${r.datatype === "int16" ? "selected" : ""}>int16</option>
            <option value="uint16" ${r.datatype === "uint16" ? "selected" : ""}>uint16</option>
            <option value="int32" ${r.datatype === "int32" ? "selected" : ""}>int32</option>
            <option value="uint32" ${r.datatype === "uint32" ? "selected" : ""}>uint32</option>
            <option value="float32" ${r.datatype === "float32" ? "selected" : ""}>float32</option>
          </select>
        </label>
        <label>Scaling: <input class="reg-scale" type="number" step="any" value="${r.scaling ?? 1.0}"></label>
        <label>Unit: <input class="reg-unit" type="text" value="${r.unit || ""}"></label>
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
  renderModBusRTUPanel("Devices");
});

/* =========================
  MODBUS TCP PANEL
========================= */

function renderModbusTcpPanel(containerId = "main-panel") {
  config.plc_configurations ??= [];

  const html = `
      <div class="panel-header" style="display:none">Modbus TCP Configuration</div>

      <div id="plc-entries-container">
        ${config.plc_configurations.map(renderPlcEntry).join("")}
      </div>

      <button id="add-plc-entry" class="button-primary">+ Add PLC</button>
      <button id="save-all-configs" class="button-primary">Save</button>
    `;

  document.getElementById(containerId).innerHTML = html;


  document.getElementById("add-plc-entry").onclick = () => {
    config.plc_configurations.push({
      plcType: "Siemens",
      PLC: {},
      isExpanded: true,
      enabled: true,   // ✅ NEW
    });
    renderModbusTcpPanel();
  };

  document.getElementById("save-all-configs").onclick = saveConfig;

  config.plc_configurations.forEach((_, i) => bindEventsForPlcEntry(i));
}

function convertToSec(val, unit) {
  if (unit === "min") return val * 60;
  if (unit === "hour") return val * 3600;
  return val;
}

function convertFromSec(sec, unit) {
  if (unit === "min") return sec / 60;
  if (unit === "hour") return sec / 3600;
  return sec;
}


/* =========================
  PLC CARD
========================= */

function bindCredFields(container, targetObj) {
  if (!container || !targetObj) return;

  container.querySelectorAll("[data-cred-field]").forEach((el) => {
    const key = el.dataset.credField;

    el.oninput = () => {
      targetObj[key] =
        el.type === "number" ? Number(el.value) : el.value;
    };
  });
}


function bindDBSettings(prefix, db) {
  const q = (name) =>
    document.querySelector(`[name="${prefix}_${name}"]`);

  const update = () => {
    db.upload_local = q("upload_local")?.checked ?? true;
    db.upload_cloud = q("upload_cloud")?.checked ?? false;
    db.db_name = q("db_name")?.value || "";
    db.table_name = q("table_name")?.value || "";
  };

  ["upload_local", "upload_cloud", "db_name", "table_name"].forEach((k) => {
    const el = q(k);
    if (el) el.oninput = update;
  });

  update(); // initial sync
}

function renderPlcEntry(plc, index) {
  const ip = plc.PLC?.cred?.ip || "Not Set";

  return `
    <div class="plc-entry" data-index="${index}">
      <div class="plc-header">
        <b>${plc.plcType}</b> – ${ip}
        <button class="toggle-plc-details">${plc.isExpanded ? "−" : "+"}</button>
      </div>

      <div class="plc-details" style="${plc.isExpanded ? "" : "display:none"}">
        <label>
          PLC Type
          <select class="plc-type-select">
            ${["Siemens", "Allen Bradley", "Delta"]
      .map(
        (t) =>
          `<option ${plc.plcType === t ? "selected" : ""}>${t}</option>`,
      )
      .join("")}
          </select>
        </label>


        ${plc.plcType === "Siemens"
      ? renderSiemensForm(plc.PLC, index)
      : renderAllenBradleyForm(plc.PLC, index)
    }

        <button class="remove-plc-entry">Remove PLC</button>
      </div>
    </div>`;
}

/* =========================
  SIEMENS
========================= */

function renderSiemensForm(plc, index) {
  plc.cred ??= { ip: "192.168.0.1", rack: 0, slot: 2 };
  plc.address_access ??= { read: [] };
  plc.data_freq_sec ??= 1;
  plc.data_freq_unit ??= "sec";
  plc.Database ??= {
    upload_local: true,
    upload_cloud: false,
    db_name: "test",
    table_name: ""
  };

  return `
    <form id="siemens-form-${index}">
      <fieldset>
        <legend>Credentials</legend>

        <label class="form-row">
          <span class="form-label">IP Address</span>
          <input type="text" data-cred-field="ip" value="${plc.cred.ip}">
        </label>

        <label class="form-row">
          <span class="form-label">Rack</span>
          <input type="number" data-cred-field="rack" value="${plc.cred.rack}">
        </label>

        <label class="form-row">
          <span class="form-label">Slot</span>
          <input type="number" data-cred-field="slot" value="${plc.cred.slot}">
        </label>
      </fieldset>
<fieldset>
  <legend>Polling</legend>

  <div style="display:flex; gap:8px; align-items:center;">
    <input type="number"
           min="0.1"
           step="0.1"
           data-freq-value
           value="${convertFromSec(plc.data_freq_sec, plc.data_freq_unit)}">

    <select data-freq-unit>
      <option value="sec" ${plc.data_freq_unit === "sec" ? "selected" : ""}>Sec</option>
      <option value="min" ${plc.data_freq_unit === "min" ? "selected" : ""}>Min</option>
      <option value="hour" ${plc.data_freq_unit === "hour" ? "selected" : ""}>Hour</option>
    </select>
  </div>
</fieldset>


      ${renderDBSettings(`plc_${index}`, plc.Database)}

      <table>
        <thead>
          <tr>
            <th>Tag</th><th>DB</th><th>Addr</th><th>Data Type</th>
            <th>Size</th><th>Min</th><th>Max</th><th>Value</th>
            <th>Read</th><th>Write</th><th></th>
          </tr>
        </thead>
        <tbody class="siemens-tbody">
          ${plc.address_access.read.map(renderSiemensRow).join("")}
        </tbody>
      </table>

      <button type="button" class="add-siemens-row">+ Add Tag</button>
    </form>`;
}


function renderSiemensRow(r, i) {
  const hideRange = r.type === "string" || r.type === "bool";

  return `
  <tr data-row-index="${i}">
    <td><input data-field="content" value="${r.content || ""}"></td>

    <td><input type="number" data-field="DB_no" value="${r.DB_no ?? 0}"></td>

    <td><input type="number" data-field="address" value="${r.address ?? 0}"></td>

    <td>
      <select data-field="type">
        ${["float", "int", "real", "bool", "string"]
      .map((t) => `<option ${r.type === t ? "selected" : ""}>${t}</option>`)
      .join("")}
      </select>
    </td>

    <!-- ✅ MANUAL SIZE -->
    <td>
      <input type="number"
            min="1"
            data-field="size"
            value="${r.size ?? ""}"
            placeholder="bytes">
    </td>

    <td style="${hideRange ? "display:none" : ""}">
      <input type="number" data-field="min" value="${r.min ?? ""}">
    </td>

    <td style="${hideRange ? "display:none" : ""}">
      <input type="number" data-field="max" value="${r.max ?? ""}">
    </td>
        <td>
  <input data-field="value" value="${r.value ?? ""}">
</td>
    <td><input type="checkbox" data-field="read" ${r.read !== false ? "checked" : ""}></td>
    <td><input type="checkbox" data-field="write" ${r.write ? "checked" : ""}></td>
    <td><button class="remove-siemens-row">−</button></td>
  </tr>`;
}


function bindSiemensFormEvents(index) {
  const plc = config.plc_configurations[index].PLC;
  const form = document.getElementById(`siemens-form-${index}`);
  if (!form) return; // 🔒 FIRST

  const tbody = form.querySelector(".siemens-tbody");
  if (!tbody) return;

  plc.cred ??= {};
  bindCredFields(form, plc.cred);
  bindDBSettings(`plc_${index}`, plc.Database);

  /* ===== POLLING ===== */
  const freqInput = form.querySelector("[data-freq-value]");
  const unitSelect = form.querySelector("[data-freq-unit]");

  if (freqInput && unitSelect) {
    const updateFreq = () => {
      const val = Number(freqInput.value) || 0;
      const unit = unitSelect.value;

      plc.data_freq_unit = unit;
      plc.data_freq_sec = convertToSec(val, unit);
    };

    freqInput.oninput = updateFreq;
    unitSelect.onchange = updateFreq;
  }

  /* ===== ROW HANDLING ===== */
  tbody.oninput = (e) => {
    const row = e.target.closest("tr");
    if (!row) return;

    const i = Number(row.dataset.rowIndex);
    const field = e.target.dataset.field;
    plc.address_access.read[i][field] =
      e.target.type === "checkbox" ? e.target.checked : e.target.value;

    if (field === "type") renderModbusTcpPanel();
  };

  tbody.onclick = (e) => {
    if (!e.target.classList.contains("remove-siemens-row")) return;

    const i = Number(e.target.closest("tr").dataset.rowIndex);
    plc.address_access.read.splice(i, 1);
    renderModbusTcpPanel();
  };

  form.querySelector(".add-siemens-row").onclick = () => {
    plc.address_access.read.push({
      content: "",
      DB_no: 1,
      address: 0,
      type: "float",
      size: "",
      min: 0,
      max: 100,
      read: true,
      write: false,
    });
    renderModbusTcpPanel();
  };
}



/* =========================
  ALLEN BRADLEY
========================= */

function renderAllenBradleyForm(plc, index) {
  plc.cred ??= { ip: "192.168.1.200", port: 44818 };
  plc.address_access ??= { read: [] };
  plc.data_freq_sec ??= 1;
  plc.data_freq_unit ??= "sec";
  plc.Database ??= {
    upload_local: true,
    upload_cloud: false,
    db_name: "test",
    table_name: ""
  };

  return `
    <form id="ab-form-${index}">
      <fieldset>
        <legend>Credentials</legend>

        <label class="form-row">
          <span class="form-label">IP</span>
          <input type="text" data-cred-field="ip" value="${plc.cred.ip}">
        </label>

        <label class="form-row">
          <span class="form-label">Port</span>
          <input type="number" data-cred-field="port" value="${plc.cred.port}">
        </label>
      </fieldset>

<fieldset>
  <legend>Polling</legend>

  <div style="display:flex; gap:8px; align-items:center;">
    <input type="number"
           min="0.1"
           step="0.1"
           data-freq-value
           value="${convertFromSec(plc.data_freq_sec, plc.data_freq_unit)}">

    <select data-freq-unit>
      <option value="sec" ${plc.data_freq_unit === "sec" ? "selected" : ""}>Sec</option>
      <option value="min" ${plc.data_freq_unit === "min" ? "selected" : ""}>Min</option>
      <option value="hour" ${plc.data_freq_unit === "hour" ? "selected" : ""}>Hour</option>
    </select>
  </div>
</fieldset>


      ${renderDBSettings(`plc_${index}`, plc.Database)}

      <table>
        <thead>
          <tr>
            <th>Tag</th><th>Addr</th><th>Data Type</th><th>Len</th>
            <th>Min</th><th>Max</th><th>Control output %</th>
            <th>Read</th><th>Write</th><th></th>
          </tr>
        </thead>
        <tbody class="ab-tbody">
          ${plc.address_access.read.map(renderAllenBradleyRow).join("")}
        </tbody>
      </table>

      <button type="button" class="add-ab-row">+ Add Tag</button>
    </form>`;
}


function renderAllenBradleyRow(r, i) {
  const isStr = r.datatype === "STRING";
  const isBool = r.datatype === "BOOL";
  const hideRange = isStr || isBool;

  return `
  <tr data-row-index="${i}">
    <td><input data-field="tag" value="${r.tag || ""}"></td>
    <td><input data-field="address" value="${r.address || ""}"></td>

    <td>
      <select data-field="datatype">
        ${["FLOAT", "INT", "REAL", "BOOL", "STRING"]
      .map(
        (t) => `<option ${r.datatype === t ? "selected" : ""}>${t}</option>`
      )
      .join("")}
      </select>
    </td>

    <td><input type="number" data-field="length" value="${r.length || 1}"></td>

    <td style="${hideRange ? "display:none" : ""}">
      <input type="number" data-field="min" value="${r.min ?? ""}">
    </td>

    <td style="${hideRange ? "display:none" : ""}">
      <input type="number" data-field="max" value="${r.max ?? ""}">
    </td>

    <td>
  <input data-field="value" value="${r.value ?? ""}"  min="0"
  max="100">
</td>


    <td><input type="checkbox" data-field="read" ${r.read !== false ? "checked" : ""}></td>
    <td><input type="checkbox" data-field="write" ${r.write ? "checked" : ""}></td>

    <td>
      <button type="button" class="remove-ab-row">−</button>
    </td>
  </tr>`;
}


function bindAllenBradleyFormEvents(index) {
  const plc = config.plc_configurations[index].PLC;
  const form = document.getElementById(`ab-form-${index}`);
  if (!form) return; // 🔒 FIRST

  plc.cred ??= {};
  bindCredFields(form, plc.cred);
  bindDBSettings(`plc_${index}`, plc.Database);

  /* ===== POLLING (SEC / MIN / HOUR) ===== */
  const freqInput = form.querySelector("[data-freq-value]");
  const unitSelect = form.querySelector("[data-freq-unit]");

  if (freqInput && unitSelect) {
    const updateFreq = () => {
      const val = Number(freqInput.value) || 0;
      const unit = unitSelect.value;

      plc.data_freq_unit = unit;
      plc.data_freq_sec = convertToSec(val, unit);
    };

    freqInput.oninput = updateFreq;
    unitSelect.onchange = updateFreq;
  }

  /* ===== TAG TABLE ===== */
  const tbody = form.querySelector(".ab-tbody");
  if (!tbody) return;

  tbody.oninput = (e) => {
    const row = e.target.closest("tr");
    if (!row) return;

    const i = Number(row.dataset.rowIndex);
    const field = e.target.dataset.field;
    let val = e.target.value;

    if (field === "value") {
      val = Math.min(100, Math.max(0, Number(val) || 0));
      e.target.value = val;
    }

    plc.address_access.read[i][field] =
      e.target.type === "checkbox" ? e.target.checked : val;
  };

  tbody.onclick = (e) => {
    if (!e.target.classList.contains("remove-ab-row")) return;

    const i = Number(e.target.closest("tr").dataset.rowIndex);
    plc.address_access.read.splice(i, 1);
    renderModbusTcpPanel();
  };

  form.querySelector(".add-ab-row").onclick = () => {
    plc.address_access.read.push({
      tag: "",
      address: "",
      datatype: "FLOAT",
      length: 1,
      min: 0,
      max: 100,
      value: "",
      read: true,
      write: false,
    });
    renderModbusTcpPanel();
  };
}



/* =========================
  COMMON BIND
========================= */

function bindEventsForPlcEntry(index) {
  const entry = document.querySelector(`.plc-entry[data-index="${index}"]`);
  if (!entry) return; // 🔒 hard guard

  const plcConfig = config.plc_configurations[index];
  if (!plcConfig) return;

  const plc = plcConfig.PLC ?? (plcConfig.PLC = {});

  /* ================= TOGGLE ================= */
  const toggleBtn = entry.querySelector(".toggle-plc-details");
  if (toggleBtn) {
    toggleBtn.onclick = () => {
      plcConfig.isExpanded = !plcConfig.isExpanded;
      renderModbusTcpPanel();
    };
  }

  /* ================= PLC TYPE ================= */
  const typeSel = entry.querySelector(".plc-type-select");
  if (typeSel) {
    typeSel.onchange = (e) => {
      plcConfig.plcType = e.target.value;
      plcConfig.PLC = {}; // reset safely
      renderModbusTcpPanel();
    };
  }

  /* ================= TABLE NAME ================= */
  const nameInput = entry.querySelector(".plc-name-input");
  if (nameInput) {
    nameInput.oninput = (e) => {
      plc.Database ??= {};
      plc.Database.table_name = e.target.value;
    };
  }

  /* ================= REMOVE ================= */
  const removeBtn = entry.querySelector(".remove-plc-entry");
  if (removeBtn) {
    removeBtn.onclick = () => {
      config.plc_configurations.splice(index, 1);
      renderModbusTcpPanel();
    };
  }

  /* ================= TYPE-SPECIFIC BINDS ================= */
  if (plcConfig.plcType === "Siemens") {
    bindSiemensFormEvents(index);
  } else if (plcConfig.plcType === "Allen Bradley" || plcConfig.plcType === "Delta") {
    bindAllenBradleyFormEvents(index);
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
    } catch (e) { }
  }
}

// ===================== Data accessors: Energy Meter =====================
function getEnergyBrands() {
  const em =
    config.ModbusRTU && config.ModbusRTU.Devices
      ? config.ModbusRTU.Devices
      : null;
  const brands = em && em.brands ? em.brands : {};
  const order =
    em && Array.isArray(em.order) && em.order.length
      ? em.order
      : Object.keys(brands);
  return { brands, order };
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
    } catch (e) { }
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

// ===================== Keep existing Digital/Analog handlers intact =====================
// setupTabHandlers should continue to attach listeners for Digital I/O and Analog cases,
// and it will not interfere with Modbus because Modbus uses its own containers and forms.


function setupTabHandlers(mainTab, subTab) {
  setTimeout(() => {
    // Digital I/O handlers
    // ---------------- DIGITAL INPUT HANDLERS ----------------
    // ---------------- DIGITAL INPUT HANDLERS ----------------
    if (mainTab === "Digital I/O") {
      let channelIndex = Number(subTab);
      if (!Number.isInteger(channelIndex)) {
        console.warn("Digital I/O subTab not numeric, forcing 0:", subTab);
        channelIndex = 0;
      }


      // 🔒 HARD INIT (this was missing)
      config.alarmSettings ??= {};
      config.alarmSettings["Digital I/O"] ??= {};
      if (!config.alarmSettings["Digital I/O"][channelIndex]) {
        config.alarmSettings["Digital I/O"][channelIndex] = { alerts: [] };
      }

      const store = config.alarmSettings["Digital I/O"][channelIndex];


      // Add
      const addBtn = document.getElementById("add-digital-alert-btn");
      if (addBtn) {
        addBtn.onclick = () => {
          store.alerts.push({
            trigger: "HIGH",
            delay: 0,
            email: "",
            contact: "",
            message: "",
            enabled: false,
          });
          renderAlarmSettings("Digital I/O", channelIndex);
        };
      }

      const form = document.getElementById("digital-io-alerts-form");
      if (!form) return;

      // Delete
      form.addEventListener("click", (e) => {
        const btn = e.target.closest(".del-alert");
        if (!btn) return;

        const idx = Number(btn.dataset.index);
        if (!Number.isInteger(idx)) return;

        store.alerts.splice(idx, 1);
        renderAlarmSettings("Digital I/O", channelIndex);
      });

      // Save
      const saveBtn = document.getElementById("save-digital-alerts");
      if (saveBtn) {
        saveBtn.onclick = () => {
          const rows = store.alerts;

          for (let i = 0; i < rows.length; i++) {
            rows[i] = {
              trigger: form[`trigger_${i}`]?.value || "HIGH",
              delay: Number(form[`delay_${i}`]?.value) || 0,
              email: form[`email_${i}`]?.value || "",
              contact: form[`contact_${i}`]?.value || "",
              message: form[`message_${i}`]?.value || "",
              enabled: !!form[`enabled_${i}`]?.checked,
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

      // Sub-tabs
      document.querySelectorAll(".sub-tab-btn").forEach((btn) => {
        btn.onclick = () => {
          renderAlarmSettings("Digital I/O", Number(btn.dataset.channel));
        };
      });
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
          renderAlarmSettings("Modbus RTU", "Alerts");
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
            renderAlarmSettings("Modbus RTU", "Alerts");
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
    // Analog handlers (RANGE-BASED)
    // ---------------- ANALOG HANDLERS ----------------
    if (mainTab === "Analog I/O") {
      const channelIndex = Number(subTab);
      if (!Number.isInteger(channelIndex)) return;

      config.alarmSettings ??= {};
      config.alarmSettings.Analog ??= {};
      config.alarmSettings.Analog[channelIndex] ??= { alerts: [] };

      const channelStore = config.alarmSettings.Analog[channelIndex];

      const channel = config.ioSettings.analog?.channels?.[channelIndex];
      if (!channel) {
        console.error("Invalid analog channel:", channelIndex);
        return;
      }

      const [min, max] = channel.range.split("-").map(Number);

      // ---- ADD ALERT ----
      const addBtn = document.getElementById("add-analog-alert-btn");
      if (addBtn) {
        addBtn.onclick = () => {
          channelStore.alerts.push({
            condition: "<=",
            threshold: min,   // valid default
            delay: 0,
            email: "",
            contact: "",
            message: "",
            enabled: false,
          });

          renderAlarmSettings("Analog I/O", channelIndex);
        };
      }

      const form = document.getElementById("analog-alerts-form");
      if (!form) return;

      // ---- DELETE ALERT ----
      form.addEventListener("click", (e) => {
        const btn = e.target.closest(".del-alert");
        if (!btn) return;

        const idx = Number(btn.dataset.index);
        if (!Number.isInteger(idx)) return;

        channelStore.alerts.splice(idx, 1);
        renderAlarmSettings("Analog I/O", channelIndex);
      });

      // ---- SAVE ALERTS ----
      const saveBtn = document.getElementById("save-analog-alerts");
      if (saveBtn) {
        saveBtn.onclick = () => {
          const rows = channelStore.alerts;

          for (let i = 0; i < rows.length; i++) {
            const cond = form[`condition_${i}`]?.value;
            const thrRaw = form[`threshold_${i}`]?.value;
            const delayRaw = form[`delay_${i}`]?.value;

            let thr = Number(thrRaw);
            if (Number.isNaN(thr)) thr = min;
            if (thr < min) {
              thr = min;
              alert("Threshold can't be less than Minimum value of range");
              return;
            }
            if (thr > max) {
              thr = max;
              alert("Threshold can't be Greater than Maximum value of range");
              return;
            }
            if (cond == "<" && thr == min) thr = min + 1;
            if (cond == ">" && thr == max) thr = max - 1;
            rows[i] = {
              condition: cond || "<=",
              threshold: thr,
              delay:
                delayRaw !== "" && !Number.isNaN(Number(delayRaw))
                  ? Number(delayRaw)
                  : 0,
              email: form[`email_${i}`]?.value || "",
              contact: form[`contact_${i}`]?.value || "",
              message: form[`message_${i}`]?.value || "",
              enabled: !!form[`enabled_${i}`]?.checked,
            };
          }

          saveConfig();   // now it WILL persist

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

function isAlarmReadOnly() {
  return uiState.role === "admin" || uiState.role === "user" || uiState.role === "guest";
}

function lockPanelViewOnly(container) {
  if (!isAlarmReadOnly()) return;

  container.querySelectorAll(
    "input, select, textarea, button"
  ).forEach(el => {
    // allow tab navigation
    if (el.classList.contains("main-tab-btn")) return;
    if (el.classList.contains("tcp-tab")) return;

    el.disabled = true;
    el.readOnly = true;
    el.style.pointerEvents = "none";
    el.style.opacity = "0.6";
  });
}


// ===================== Modbus composite UI: Energy + PLC =====================
function renderAlarmSettings(mainTab = "Modbus RTU", subTab = "Channel 1") {
  migrateModbusAlertsToNested();

  // const MainTabs = ["Digital I/O", "Modbus", "Analog I/O", "SMS Settings"];
  const MainTabs = ["Modbus RTU", "Modbus TCP", "Analog I/O", "Digital I/O"];
  const mainTabsHtml = MainTabs.map(
    (t) =>
      `<button class="main-tab-btn${t === mainTab ? " active" : ""
      }" data-tab="${t}" style="${tabBtnStyle(t === mainTab)}"
              onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
              onmouseleave="if(!this.classList.contains('active')) this.style.background='${UI_TAB_ACTIVE_BG}'"
            >${t}</button>`,
  ).join("");

  const panel = document.getElementById("main-panel");
  panel.innerHTML = `
          <div style="font-size:16px; font-weight:600; color:${UI_TEXT}; margin:4px 0 8px;">Alarms</div>
          <div class="tabs-line" style="display:flex; flex-wrap:wrap; align-items:center;">${mainTabsHtml}</div>
          <div class="panel-content" style="margin-top: 12px;">
            ${renderAlarmSettingsContent(mainTab, subTab)}
          </div>
        `;
  // ---- Modbus RTU ----
  if (mainTab === "Modbus RTU") {
    try {
      renderModbusRTUDeviceSection();
    } catch (e) {
      console.error(e);
    }
  }

  // ---- Modbus TCP ----
  if (mainTab === "Modbus TCP") {
    const body = document.getElementById("modbus-tcp-body");
    if (!body) return;
    initTcpAlarmDraft();
    const types = getPlcTypes();
    const defaultType = types[0];
    body.innerHTML = renderModbusTcpPlcList(defaultType);


    document.querySelectorAll(".tcp-tab").forEach((btn) => {
      btn.onclick = () => {
        // remove active from all
        document
          .querySelectorAll(".tcp-tab")
          .forEach((b) => b.classList.remove("active"));

        // set active
        btn.classList.add("active");

        body.innerHTML = renderModbusTcpPlcList(btn.dataset.type);
        bindTcpAlarmHandlers(btn.dataset.type);
      };
    });

    bindTcpAlarmHandlers(defaultType);
  }

  // try {
  //   renderPlcSection();
  // } catch (e) {
  //   console.error(e);
  // }

  // If preferring next-frame timing, use:
  // requestAnimationFrame(() => {
  //   try { renderModbusRTUDeviceSection(); } catch(e) { console.error(e); }
  //   try { renderPlcSection(); } catch(e) { console.error(e); }
  // });

  document.querySelectorAll(".main-tab-btn").forEach(
    (btn) =>
    (btn.onclick = () => {
      const newMainTab = btn.dataset.tab;
      let defaultSub = subTab;
      if (newMainTab === "Digital I/O") defaultSub = "0";
      if (newMainTab === "Analog I/O") defaultSub = "0";
      if (newMainTab === "Modbus RTU") defaultSub = "Energy Meter";
      renderAlarmSettings(newMainTab, defaultSub);
    }),
  );

  setupTabHandlers(mainTab, subTab);

  // 🔒 FINAL LINE — THIS IS THE KEY
  lockPanelViewOnly(panel);

}

let tcpHandlersBound = false;

function resolvePlcTagLabel(plcType, tag) {
  return (
    tag.tag ??
    tag.content ??
    tag.name ??
    tag.address ??
    "UNKNOWN_TAG"
  );
}


function getPlcTypes() {
  const list = config.plc_configurations || [];
  return [...new Set(list.map(p => p.plcType))];
}

function ensureTcpAlerts(plcType, plcKey, tagName) {
  tcpAlarmDraft ??= {};

  tcpAlarmDraft[plcType] ??= {};
  tcpAlarmDraft[plcType][plcKey] ??= {};
  tcpAlarmDraft[plcType][plcKey][tagName] ??= [];

  return tcpAlarmDraft[plcType][plcKey][tagName];
}


function renderTcpBody(plcType) {
  const body = document.getElementById("modbus-tcp-body");
  body.innerHTML = renderModbusTcpPlcList(plcType);

  document.querySelectorAll(".tcp-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.type === plcType);
    btn.onclick = () => renderTcpBody(btn.dataset.type);
  });
}

function renderModbusTcpPlcList(plcType) {
  const list = config.plc_configurations.filter((p) => p.plcType === plcType);

  if (!list.length) return `<div>No ${plcType} PLCs configured</div>`;

  return list
    .map((plc, idx) => {
      const plcKey =
        plc.PLC?.Database?.table_name ||
        plc.PLC?.cred?.ip ||
        `${plcType}_${idx}`;

      return `
        <div class="tcp-plc-card">
          <div class="tcp-plc-header">
            <b class="tcp-plc-title">${plcKey}</b>
            <button class="tcp-save">Save</button>
          </div>
          ${renderTcpPlcTags(plcType, plcKey, plc)}
        </div>
      `;
    })
    .join("");
}

function renderTcpPlcTags(plcType, plcKey, plc) {
  const reads =
    (plc.PLC?.address_access?.read || []).filter(t => t.read === true);

  if (!reads.length) {
    return `<i>No readable tags configured</i>`;
  }

  return reads
    .map((tag) => {
      const label = resolvePlcTagLabel(plcType, tag);
      return `
        <div class="tcp-tag"
      data-plc-type="${plcType}"
      data-plc="${plcKey}"
      data-tag="${label}">

          <strong>${label}</strong>
          ${renderTcpAlarmTable(plcType, plcKey, label, tag)}
        </div>
      `;
    })
    .join("");
}

function renderTcpAlarmTable(plcType, plcKey, tagName, tag) {
  const alerts = ensureTcpAlerts(plcType, plcKey, tagName);

  return `
      <table class="tcp-alert-table">
        <thead>
          <tr>
            <th>Cond</th><th>Threshold</th><th>Email</th><th>Contact</th>  
            <th>Message</th><th>On</th><th></th>
          </tr>
        </thead>
        <tbody>
          ${alerts
      .map(
        (a, i) => `
            <tr data-i="${i}">
              <td>
                <select class="cond">
                  ${["<=", "<", ">=", ">"]
            .map(
              (c) =>
                `<option ${a.condition === c ? "selected" : ""}>${c}</option>`,
            )
            .join("")}
                </select>
              </td>
              <td><input class="thr" type="number" value="${a.threshold ?? ""}"></td>
              <td><input class="email" value="${a.email || ""}"></td>
              <td>
    <input class="contact"
          placeholder="Mobile Number"
          value="${a.contact || ""}">
  </td>
              <td><input class="msg" value="${a.message || ""}"></td>
              <td><input class="en" type="checkbox" ${a.enabled ? "checked" : ""}></td>
              <td><button class="del">×</button></td>
            </tr>
          `,
      )
      .join("")}
        </tbody>
      </table>
      <button class="add-tcp-alert">+ Add Alert</button>
    `;
}

function validateTcpThreshold(tag, cond, value) {
  if (value === null || value === "") return;

  if ((cond === "<" || cond === "<=") && tag.min != null && value < tag.min)
    throw new Error(`Threshold ${value} < min ${tag.min}`);

  if ((cond === ">" || cond === ">=") && tag.max != null && value > tag.max)
    throw new Error(`Threshold ${value} > max ${tag.max}`);
}



let tcpAlarmDraft = null;

function initTcpAlarmDraft() {
  tcpAlarmDraft = deepClone(config.alarmSettings?.ModbusTCP || {});
}


function bindTcpAlarmHandlers() {
  if (tcpHandlersBound) return;
  tcpHandlersBound = true;

  const body = document.getElementById("modbus-tcp-body");
  if (!body) return;

  body.addEventListener("click", (e) => {
    /* ================= SAVE (PLC LEVEL) ================= */
    const saveBtn = e.target.closest(".tcp-save");
    if (saveBtn) {
      try {
        config.alarmSettings ??= {};
        config.alarmSettings.ModbusTCP = deepClone(tcpAlarmDraft);

        saveConfig();

        saveBtn.textContent = "Saved ✓";
        setTimeout(() => (saveBtn.textContent = "Save"), 900);
      } catch (err) {
        console.error(err);
        alert("Failed to save Modbus TCP alarms");
      }
      return;
    }


    /* ================= TAG-LEVEL ACTIONS ================= */
    const tagDiv = e.target.closest(".tcp-tag");
    if (!tagDiv) return;

    const plcType = tagDiv.dataset.plcType;
    const plcKey = tagDiv.dataset.plc;
    const tagName = tagDiv.dataset.tag;

    if (!plcType || !plcKey || !tagName) return;

    /* ----- ADD ALERT ----- */
    if (e.target.classList.contains("add-tcp-alert")) {
      ensureTcpAlerts(plcType, plcKey, tagName).push({
        condition: "<=",
        threshold: "",
        email: "",
        contact: "",          // ✅ ADDED
        message: `${plcKey} ${tagName}`,
        enabled: true,
      });
      renderTcpBody(plcType);
      return;
    }

    /* ----- DELETE ALERT ----- */
    if (e.target.classList.contains("del")) {
      const tr = e.target.closest("tr");
      if (!tr) return;

      const idx = Number(tr.dataset.i);
      const list = ensureTcpAlerts(plcType, plcKey, tagName);

      if (Number.isInteger(idx) && idx >= 0 && idx < list.length) {
        list.splice(idx, 1);
        renderTcpBody(plcType);
      }
      return;
    }
  });

  /* ================= LIVE EDIT ================= */
  body.addEventListener("change", (e) => {
    const tr = e.target.closest("tr");
    if (!tr) return;

    const tagDiv = e.target.closest(".tcp-tag");
    if (!tagDiv) return;

    const plcType = tagDiv.dataset.plcType;
    const plcKey = tagDiv.dataset.plc;
    const tagName = tagDiv.dataset.tag;

    const idx = Number(tr.dataset.i);
    if (!Number.isInteger(idx)) return;

    const list = ensureTcpAlerts(plcType, plcKey, tagName);
    const alertObj = list[idx];
    if (!alertObj) return;

    const cond = tr.querySelector(".cond")?.value || "<=";
    const thrRaw = tr.querySelector(".thr")?.value;
    const thr = thrRaw === "" ? null : Number(thrRaw);

    // 🔥 FIND PLC TAG (THIS WAS MISSING)
    const plcTag = findPlcTag(plcType, plcKey, tagName);

    try {
      if (!plcTag) {
        throw new Error(`PLC tag "${tagName}" not found`);
      }

      // 🔥 NULL CHECK (YOU ASKED FOR THIS)
      if (thr === null || Number.isNaN(thr)) {
        throw new Error("Threshold value is required");
      }

      // 🔥 NORMALIZE min/max
      const min =
        plcTag.min !== undefined && plcTag.min !== ""
          ? Number(plcTag.min)
          : null;

      const max =
        plcTag.max !== undefined && plcTag.max !== ""
          ? Number(plcTag.max)
          : null;

      // 🔥 VALIDATION
      if (thr < min || thr > max) {
        throw new Error(`Threshold ${thr} is not between  ${min} & ${max}`);
      }
      if ((cond === "<" || cond === "<=") && min !== null && thr < min) {
        throw new Error(`Threshold ${thr} is less than minimum ${min}`);
      }

      if ((cond === ">" || cond === ">=") && max !== null && thr > max) {
        throw new Error(`Threshold ${thr} is greater than maximum ${max}`);
      }


      // ✅ ONLY UPDATE IF VALID
      alertObj.condition = cond;
      alertObj.threshold = thrRaw;
      alertObj.email = tr.querySelector(".email")?.value || "";
      alertObj.contact = tr.querySelector(".contact")?.value || "";
      alertObj.message = tr.querySelector(".msg")?.value || "";
      alertObj.enabled = !!tr.querySelector(".en")?.checked;


    } catch (err) {
      alert(err.message);

      // 🔁 Re-render to reset invalid input
      renderTcpBody(plcType);
    }
  });

}

function findPlcTag(plcType, plcKey, tagName) {
  const plc = config.plc_configurations.find((p) => {
    if (p.plcType !== plcType) return false;

    const key =
      p.PLC?.Database?.table_name ||
      p.PLC?.cred?.ip;

    return key === plcKey;
  });

  if (!plc) return null;

  return plc.PLC?.address_access?.read?.find((r) => {
    if (r.read !== true) return false;

    const resolved =
      r.tag ??
      r.content ??
      r.name ??
      r.address;

    return resolved === tagName;
  }) || null;
}


function renderTcpTabs(activeType) {
  const types = getPlcTypes();
  if (!types.length) return `<i>No PLCs configured</i>`;

  return `
    <div class="tcp-tabs">
      ${types.map(t => `
        <button
          class="tcp-tab ${t === activeType ? "active" : ""}"
          data-type="${t}">
          ${t}
        </button>
      `).join("")}
    </div>
  `;
}


function renderAlarmSettingsContent(mainTab, subTab) {
  if (mainTab === "Digital I/O") {
    return renderDigitalIOAlertsForm(subTab);
  }
  if (mainTab === "Analog I/O") {
    const chIndex = typeof subTab === "number" ? subTab : 0;
    return renderAnalogAlertsForm(chIndex);
  }

  if (mainTab === "Modbus TCP") {
    const types = getPlcTypes();
    const defaultType = types[0];
    initTcpAlarmDraft();

    return `
    ${renderTcpTabs(defaultType)}
    <div id="modbus-tcp-body"></div>
  `;
  }


  if (mainTab === "SMS Settings") {
    return `<div style="padding: 20px; color: #666;">SMS Settings UI coming soon</div>`;
  }
  if (mainTab === "Modbus RTU") {
    return `
            <div style="margin:14px 0 10px; color:${UI_TEXT}; font-weight:600;"></div>
            <div id="em-section"></div>
            <div style="margin:18px 0 10px; color:${UI_TEXT}; font-weight:600;"></div>
            <div id="plc-section"></div>
          `;
  }
  return `<div style="padding: 20px;">Please select a tab</div>`;
}

function renderDigitalIOChannelTabs(activeChannel) {
  const channels = config.ioSettings.digitalInput?.channels || [];

  return `
    <div class="tabs-line" style="margin-bottom:10px;">
      ${channels
      .map(
        (ch, i) => `
        <button class="sub-tab-btn${activeChannel === i ? " active" : ""}"
                data-channel="${i}">
          ${ch.name || `DI${i + 1}`}
        </button>
      `
      )
      .join("")}
    </div>
  `;
}


function renderDigitalIOAlertsForm(channelIndex) {
  if (!config.ioSettings.digitalInput || !Array.isArray(config.ioSettings.digitalInput.channels)) {
    return `<div style="padding:16px;color:#c00;">Digital Input not configured</div>`;
  }

  channelIndex = Number(channelIndex);
  if (!Number.isInteger(channelIndex)) channelIndex = 0;

  const channel = config.ioSettings.digitalInput.channels[channelIndex];
  if (!channel) {
    return `<div style="padding:16px;color:#c00;">Invalid channel ${channelIndex}</div>`;
  }

  config.alarmSettings ??= {};
  config.alarmSettings["Digital I/O"] ??= {};
  if (!config.alarmSettings["Digital I/O"][channelIndex]) {
    config.alarmSettings["Digital I/O"][channelIndex] = { alerts: [] };
  }

  const store = config.alarmSettings["Digital I/O"][channelIndex];

  const rows = store.alerts
    .map(
      (row, i) => `
      <tr>
        <td>${i + 1}</td>

        <td>
          <select name="trigger_${i}">
            <option value="HIGH" ${row.trigger === "HIGH" ? "selected" : ""}>
              HIGH
            </option>
            <option value="LOW" ${row.trigger === "LOW" ? "selected" : ""}>
              LOW
            </option>
          </select>
        </td>

        <td><input type="number" name="delay_${i}" value="${row.delay ?? 0}" style="width:80px"></td>
        <td><input type="email" name="email_${i}" value="${row.email ?? ""}" style="width:140px"></td>
        <td><input type="text" name="contact_${i}" value="${row.contact ?? ""}" style="width:140px"></td>
        <td><input type="text" name="message_${i}" value="${row.message ?? ""}" style="width:160px"></td>

        <td style="text-align:center">
          <input type="checkbox" name="enabled_${i}" ${row.enabled ? "checked" : ""}>
        </td>

        <td style="text-align:center">
          <button type="button" class="del-alert" data-index="${i}">×</button>
        </td>
      </tr>
    `
    )
    .join("");

  return `
    ${renderDigitalIOChannelTabs(channelIndex)}

    <form id="digital-io-alerts-form">
      <button type="button" class="button-primary" id="add-digital-alert-btn">
        + Add Alert
      </button>

      <table class="channel-table" style="width:100%; background:#fff; margin-top:10px;">
        <tr>
          <th>#</th>
          <th>Trigger</th>
          <th>Delay (s)</th>
          <th>Email</th>
          <th>Contact</th>
          <th>Message</th>
          <th>Enabled</th>
          <th>Delete</th>
        </tr>
        ${rows}
      </table>

      <div style="margin-top:12px;">
        <button type="button" class="button-primary" id="save-digital-alerts">
          Save
        </button>
        <span class="success-tick" id="digital-io-alerts-tick" style="display:none;">✔</span>
      </div>
    </form>
  `;
}


function renderAnalogChannelTabs(activeChannel) {
  const channels = config.ioSettings.analog?.channels || [];

  return `
    <div class="tabs-line" style="margin-bottom:10px;">
      ${channels
      .map(
        (ch, i) => `
        <button class="sub-tab-btn${activeChannel === i ? " active" : ""}"
                data-channel="${i}">
          ${ch.name || `A${i + 1}`}
        </button>
      `,
      )
      .join("")}
    </div>
  `;
}


function renderAnalogAlertsForm(channelIndex) {
  if (!config.ioSettings.analog || !Array.isArray(config.ioSettings.analog.channels)) {
    return `<div style="padding:16px;color:#c00;">Analog not configured</div>`;
  }

  channelIndex = Number(channelIndex);
  if (!Number.isInteger(channelIndex)) channelIndex = 0;

  const channel = config.ioSettings.analog.channels[channelIndex];
  if (!channel) {
    return `<div style="padding:16px;color:#c00;">Invalid channel ${channelIndex}</div>`;
  }

  const [min, max] = channel.range
  .match(/-?\d+(\.\d+)?/g)
  ?.map(Number) || [];


  config.alarmSettings ??= {};
  config.alarmSettings.Analog ??= {};
  if (!config.alarmSettings.Analog[channelIndex]) {
    config.alarmSettings.Analog[channelIndex] = { alerts: [] };
  }

  const store = config.alarmSettings.Analog[channelIndex];

  const rows = store.alerts
    .map(
      (row, i) => `
      <tr>
        <td>${i + 1}</td>

        <td>
          <select name="condition_${i}">
            <option value="<"  ${row.condition === "<" ? "selected" : ""}>&lt;</option>
            <option value="<=" ${row.condition === "<=" ? "selected" : ""}>&le;</option>
            <option value=">"  ${row.condition === ">" ? "selected" : ""}>&gt;</option>
            <option value=">=" ${row.condition === ">=" ? "selected" : ""}>&ge;</option>
          </select>
        </td>

        <td>
          <input type="number"
                 name="threshold_${i}"
                 step="any"
                 min="${min}"
                 max="${max}"
                 value="${row.threshold ?? min}"
                 style="width:80px">
          <div style="font-size:11px;color:#888;">Range: ${min} – ${max}</div>
        </td>

        <td><input type="number" name="delay_${i}" value="${row.delay ?? 0}" style="width:80px"></td>
        <td><input type="email" name="email_${i}" value="${row.email ?? ""}" style="width:140px"></td>
        <td><input type="text" name="contact_${i}" value="${row.contact ?? ""}" style="width:140px"></td>
        <td><input type="text" name="message_${i}" value="${row.message ?? ""}" style="width:160px"></td>

        <td style="text-align:center">
          <input type="checkbox" name="enabled_${i}" ${row.enabled ? "checked" : ""}>
        </td>

        <td style="text-align:center">
          <button type="button" class="del-alert" data-index="${i}">×</button>
        </td>
      </tr>
    `
    )
    .join("");

  return `
    ${renderAnalogChannelTabs(channelIndex)}

    <form id="analog-alerts-form">
      <button type="button" class="button-primary" id="add-analog-alert-btn">
        + Add Alert
      </button>

      <table class="channel-table" style="width:100%;background:#fff;margin-top:10px;">
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

      <div style="margin-top:12px;">
        <button type="button" class="button-primary" id="save-analog-alerts">
          Save
        </button>
        <span class="success-tick" id="analog-alerts-tick" style="display:none;">✔</span>
      </div>
    </form>
  `;
}



// Updated helper functions for Energy Meter registers and alerts
function getEnergyBrandRegisters(brandKey, slaveId) {
  const { brands } = getEnergyBrands();
  return brands[brandKey]?.registersBySlave?.[slaveId] || [];
}

function getEnergyBrandSlaves(brandKey) {
  const { brands } = getEnergyBrands();
  return brands[brandKey]?.slaves?.map((s) => s.id.toString()) || [];
}

function migrateModbusAlertsIfNeeded() {
  const mod = config.alarmSettings?.Modbus;
  if (!mod) return;

  Object.entries(mod).forEach(([brandKey, brand]) => {
    Object.entries(brand.slaves || {}).forEach(([slaveId, slave]) => {
      // 🔴 OLD FORMAT DETECTED
      if (Array.isArray(slave.alerts)) {
        const newAlerts = {};
        const regs =
          config.ModbusRTU?.Devices?.brands?.[brandKey]
            ?.registersBySlave?.[slaveId] || [];

        slave.alerts.forEach((alert) => {
          // Try to match by register name in message
          const matchedReg = regs.find((r) =>
            alert.message?.includes(r.name),
          );

          if (!matchedReg) return;

          const key = getRegisterKey(matchedReg);
          newAlerts[key] ??= [];
          newAlerts[key].push(alert);
        });

        slave.alerts = newAlerts; // 🔥 MIGRATED
      }
    });
  });
}


// ===================== Updated Energy Meter with Register Tabs =====================
function renderModbusRTUDeviceSection(
  selectedBrand = "",
  selectedSlave = "",
  selectedRegisterKey = "",
) {
  migrateModbusAlertsIfNeeded();

  const mount = document.getElementById("em-section");
  if (!mount) return;

  const { brands, order } = getEnergyBrands();

  if (!selectedBrand || !selectedSlave || !selectedRegisterKey) {
    const pick = pickFirstEnergyBrandAndSlave();
    selectedBrand ||= pick.brandKey;
    selectedSlave ||= pick.slaveId;
    const firstReg = getEnergyBrandRegisters(selectedBrand, selectedSlave)[0];
    selectedRegisterKey ||= firstReg ? getRegisterKey(firstReg) : "";
  }

  const brandTabs = (order.length ? order : Object.keys(brands))
    .map(
      (bk) =>
        `<button class="em-brand" data-brand="${bk}" style="${tabBtnStyle(
          bk === selectedBrand,
        )}">${brands[bk]?.label || bk}</button>`,
    )
    .join("");

  const slaveTabs = getEnergyBrandSlaves(selectedBrand)
    .map(
      (sid) =>
        `<button class="em-slave" data-slave="${sid}" style="${tabBtnStyle(
          sid === selectedSlave,
        )}">Slave ${sid}</button>`,
    )
    .join("");

  const regs = getEnergyBrandRegisters(selectedBrand, selectedSlave);

  const registerTabs = regs
    .map((reg) => {
      const key = getRegisterKey(reg);
      return `<button class="em-register"
                data-register="${key}"
                style="${tabBtnStyle(key === selectedRegisterKey)}">
                ${reg.name}
              </button>`;
    })
    .join("");

  mount.innerHTML = `
      <div>${brandTabs}</div>
      <div style="margin-top:6px">${slaveTabs}</div>
      <div style="margin-top:6px">${registerTabs}</div>
      <div style="margin-top:10px">
        ${renderEnergyAlertsTable(
    selectedBrand,
    selectedSlave,
    selectedRegisterKey,
  )}
      </div>
    `;

  mount.querySelectorAll(".em-brand").forEach(
    (b) =>
    (b.onclick = () =>
      renderModbusRTUDeviceSection(b.dataset.brand, "", "")),
  );

  mount.querySelectorAll(".em-slave").forEach(
    (b) =>
    (b.onclick = () =>
      renderModbusRTUDeviceSection(
        selectedBrand,
        b.dataset.slave,
        "",
      )),
  );

  mount.querySelectorAll(".em-register").forEach(
    (b) =>
    (b.onclick = () =>
      renderModbusRTUDeviceSection(
        selectedBrand,
        selectedSlave,
        b.dataset.register,
      )),
  );

  setupEnergyHandlers(selectedBrand, selectedSlave, selectedRegisterKey);
}

function getRegisterProcessRange(brandKey, slaveId, registerKey) {
  const regs = getEnergyBrandRegisters(brandKey, slaveId);

  const reg = regs.find(
    (r) => getRegisterKey(r) === registerKey
  );

  if (!reg) return { min: null, max: null };

  const min =
    reg.process_min !== undefined && reg.process_min !== ""
      ? Number(reg.process_min)
      : null;

  const max =
    reg.process_max !== undefined && reg.process_max !== ""
      ? Number(reg.process_max)
      : null;

  return { min, max };
}

function getModbusAlerts(brandKey, slaveId, registerKey) {
  return (
    config.alarmSettings?.Modbus?.[brandKey]
      ?.slaves?.[slaveId]
      ?.alerts?.[registerKey] || []
  );
}

function getColumnNameFromRegister(registerKey) {
  const [start, offset, type, length] = registerKey.split("|");
  return `${type}_${start}_${offset}`; // OR whatever your DB column rule is
}


function renderEnergyAlertsTable(brandKey, slaveId, registerKey) {
  const regs = getEnergyBrandRegisters(brandKey, slaveId);
  const reg = regs.find(r => getRegisterKey(r) === registerKey);
  if (!reg) return "<div>Invalid register</div>";
  
  const alertKey = buildRtuAlertKey(brandKey, slaveId, reg);
  const alerts = getRtuAlerts(alertKey);

  const rowsHtml = alerts.length
    ? alerts
      .map(
        (row, i) => `
  <tr data-row="${i}">
    <td>${i + 1}</td>

    <td>
      <select name="condition_${i}">
        <option value="<=" ${row.condition === "<=" ? "selected" : ""}>&le;</option>
        <option value="<"  ${row.condition === "<" ? "selected" : ""}>&lt;</option>
        <option value=">=" ${row.condition === ">=" ? "selected" : ""}>&ge;</option>
        <option value=">"  ${row.condition === ">" ? "selected" : ""}>&gt;</option>
      </select>
    </td>

    <td>
      <input type="number" step="any"
            name="threshold_${i}"
            value="${row.threshold ?? ""}"
            placeholder="Value">
    </td>

    <td>
      <input type="email"
            name="email_${i}"
            value="${row.email ?? ""}"
            placeholder="email@example.com">
    </td>

    <td>
      <input type="text"
            name="contact_${i}"
            value="${row.contact ?? ""}"
            placeholder="Mobile Number">
    </td>

    <td>
      <input type="text"
            name="message_${i}"
            value="${row.message ?? ""}"
            placeholder="Alert message">
    </td>

    <td>
  <select name="do_${i}" class="do-select">
    <option value="neutral" ${!row.digital_output || row.digital_output === "neutral" ? "selected" : ""}>Neutral</option>
    <option value="DO1" ${row.digital_output === "DO1" ? "selected" : ""}>DO1</option>
    <option value="DO2" ${row.digital_output === "DO2" ? "selected" : ""}>DO2</option>
    <option value="DO3" ${row.digital_output === "DO3" ? "selected" : ""}>DO3</option>
    <option value="DO4" ${row.digital_output === "DO4" ? "selected" : ""}>DO4</option>
  </select>
</td>

<td>
  <select name="status_${i}" class="status-select">
    <option value="NA" ${!row.status || row.status === "NA" ? "selected" : ""}>NA</option>
    <option value="HIGH" ${row.status === "HIGH" ? "selected" : ""}>HIGH</option>
    <option value="LOW" ${row.status === "LOW" ? "selected" : ""}>LOW</option>
  </select>
</td>
    <td style="text-align:center">
      <input type="checkbox" name="enabled_${i}" ${row.enabled ? "checked" : ""}>
    </td>

    <td style="text-align:center">
      <button type="button"
              class="em-del"
              data-idx="${i}"
              style="border:1px solid #ef4444;
                    color:#ef4444;
                    background:#fff;
                    padding:4px 8px;
                    border-radius:4px;
                    cursor:pointer;">
        ✕
      </button>
    </td>
  </tr>
  `,
      )
      .join("")
    : `
  <tr>
    <td colspan="10" style="padding:14px; text-align:center; color:#6b7280;">
      No alerts configured for this register.
    </td>
  </tr>
  `;

  return `
  <form id="em-form" onsubmit="return false;">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <div style="font-size:13px; color:#6b7280;">
        Alerts for <b>${brandKey}</b> · Slave <b>${slaveId}</b>
      </div>

      <div style="display:flex; gap:8px;">
        <button type="button" id="em-add" class="button-primary">+ Add Alert</button>
        <button type="submit" id="em-save" class="button">Save</button>
        <span id="em-tick" style="display:none; color:#22c55e; font-weight:600;">Saved ✔</span>
      </div>
    </div>

    <div style="overflow:auto; border:1px solid #e5e7eb; border-radius:8px;">
      <table style="width:100%; border-collapse:collapse;">
        <thead style="background:#f9fafb;">
          <tr>
            <th>#</th>
            <th>Condition</th>
            <th>Threshold</th>
            <th>Email</th>
            <th>Contact</th>
            <th>Message</th>
            <th>Digital Output</th>
<th>Status</th>
            <th>Enabled</th>
            <th>Delete</th>
          </tr>
        </thead>

        <tbody>
          ${rowsHtml}
        </tbody>
      </table>
    </div>
  </form>
  `;
}

function getRegisterKey(reg) {
  return `${reg.start}|${reg.offset}|${reg.type}|${reg.length}`;
}

function ensureModbusAlerts(brandKey, slaveId, registerKey) {
  config.alarmSettings ??= {};
  config.alarmSettings.Modbus ??= {};
  const mod = config.alarmSettings.Modbus;

  mod[brandKey] ??= {};
  mod[brandKey].slaves ??= {};
  mod[brandKey].slaves[slaveId] ??= {};
  mod[brandKey].slaves[slaveId].alerts ??= {};

  if (!Array.isArray(mod[brandKey].slaves[slaveId].alerts[registerKey])) {
    mod[brandKey].slaves[slaveId].alerts[registerKey] = [];
  }

  return mod[brandKey].slaves[slaveId].alerts[registerKey];
}

function setModbusAlerts(brandKey, slaveId, registerKey, alerts) {
  ensureModbusAlerts(brandKey, slaveId, registerKey);
  config.alarmSettings.Modbus[brandKey]
    .slaves[slaveId]
    .alerts[registerKey] = alerts;
}

function buildRtuAlertKey(brandKey, slaveId, reg) {
  return `${brandKey}_S${slaveId}_${reg.name}`;
}

function ensureRtuAlerts(alertKey) {
  config.alarmSettings ??= {};
  config.alarmSettings.ModbusRTU ??= {};

  if (!Array.isArray(config.alarmSettings.ModbusRTU[alertKey])) {
    config.alarmSettings.ModbusRTU[alertKey] = [];
  }
  return config.alarmSettings.ModbusRTU[alertKey];
}

function getRtuAlerts(alertKey) {
  return config.alarmSettings?.ModbusRTU?.[alertKey] || [];
}

function setRtuAlerts(alertKey, alerts) {
  config.alarmSettings.ModbusRTU[alertKey] = alerts;
}

function setupEnergyHandlers(brandKey, slaveId, registerKey) {
  const form = document.getElementById("em-form");
  if (!form) return;

  function updateStatusControl(rowIndex) {
  const doEl = form.querySelector(`[name=do_${rowIndex}]`);
  const statusEl = form.querySelector(`[name=status_${rowIndex}]`);

  if (!doEl || !statusEl) return;

  if (doEl.value === "neutral") {
    statusEl.value = "NA";
    statusEl.disabled = true;
  } else {
    statusEl.disabled = false;

    if (statusEl.value === "NA") {
      statusEl.value = "HIGH"; // default
    }
  }
}

// initialize + attach listeners
form.querySelectorAll(".do-select").forEach((el, i) => {
  updateStatusControl(i);
  el.addEventListener("change", () => updateStatusControl(i));
});

  const regs = getEnergyBrandRegisters(brandKey, slaveId);
const reg = regs.find(r => getRegisterKey(r) === registerKey);
if (!reg) return;

const alertKey = buildRtuAlertKey(brandKey, slaveId, reg);


  form.querySelectorAll("input, select, textarea").forEach(el => {
    el.addEventListener("click", e => e.stopPropagation());
  });


  form.onclick = (e) => {
    if (e.target.id === "em-add") {
      //const columnName = getColumnNameFromRegister(registerKey);
      const alertKey = buildRtuAlertKey(brandKey, slaveId, reg);
      
      ensureRtuAlerts(alertKey).push({
        condition: "<=",
        threshold: "",
        email: "",
        contact: "",
        message: `${brandKey} S${slaveId}`,
          digital_output: "neutral",
  status: "NA",
        enabled: true,
      });
      
      saveConfig();
      renderModbusRTUDeviceSection(brandKey, slaveId, registerKey);
    }

    if (e.target.classList.contains("em-del")) {
      const i = Number(e.target.dataset.idx);
      ensureRtuAlerts(alertKey).splice(i, 1);
      saveConfig();
      renderModbusRTUDeviceSection(brandKey, slaveId, registerKey);
    }
  };
  form.onsubmit = (e) => {
    e.preventDefault();

    const { min, max } = getRegisterProcessRange(
      brandKey,
      slaveId,
      registerKey
    );
    
    
    const trs = [...form.querySelectorAll("tbody tr")];

    // 🔒 Placeholder-only table → do nothing
    if (
      trs.length === 1 &&
      trs[0].querySelectorAll("input, select").length === 0
    ) {
      alert("No alert rows to save.");
      return;
    }

    const alerts = [];

    for (let i = 0; i < trs.length; i++) {
      const tr = trs[i];

      const conditionEl = tr.querySelector(`[name=condition_${i}]`);
      if (!conditionEl) continue;

      const condition = conditionEl.value;
      const thresholdRaw = tr.querySelector(`[name=threshold_${i}]`)?.value;
      const message = tr.querySelector(`[name=message_${i}]`)?.value?.trim();
      const email = tr.querySelector(`[name=email_${i}]`)?.value?.trim();
      const contact = tr.querySelector(`[name=contact_${i}]`)?.value?.trim();
      const enabled = tr.querySelector(`[name=enabled_${i}]`)?.checked || false;
      const digital_output = tr.querySelector(`[name=do_${i}]`)?.value || "neutral";
const status = tr.querySelector(`[name=status_${i}]`)?.value || "NA";

      // 🔥 NULL / EMPTY CHECKS (HARD FAIL)
      if (!condition) {
        alert(`Row ${i + 1}: Condition is required.`);
        return;
      }

      if (thresholdRaw === "" || thresholdRaw === null) {
        alert(`Row ${i + 1}: Threshold is required.`);
        return;
      }

      if (!message) {
        alert(`Row ${i + 1}: Message cannot be empty.`);
        return;
      }

      if (!email && !contact) {
        alert(
          `Row ${i + 1}: Either Email or Contact must be provided.`
        );
        return;
      }
if (digital_output === "neutral" && status !== "NA") {
  alert(`Row ${i + 1}: Status must be NA when Digital Output is Neutral.`);
  return;
}

if (digital_output !== "neutral" && (status !== "HIGH" && status !== "LOW")) {
  alert(`Row ${i + 1}: Status must be HIGH or LOW when Digital Output is selected.`);
  return;
}
      const threshold = Number(thresholdRaw);
      if (Number.isNaN(threshold)) {
        alert(`Row ${i + 1}: Threshold must be a valid number.`);
        return;
      }

      // 🔥 PROCESS RANGE VALIDATION
      if (
        min !== null &&
        (condition === "<" || condition === "<=") &&
        threshold < min
      ) {
        alert(
          `Row ${i + 1}: Threshold ${threshold} is below process minimum ${min}`
        );
        return;
      }

      if (
        max !== null &&
        (condition === ">" || condition === ">=") &&
        threshold > max
      ) {
        alert(
          `Row ${i + 1}: Threshold ${threshold} exceeds process maximum ${max}`
        );
        return;
      }

      alerts.push({
        condition,
        threshold: thresholdRaw,
        email,
        contact,
        message,
          digital_output,
  status,
        enabled,
      });
    }

    // 🔒 NEVER overwrite with empty array
    if (alerts.length === 0) {
      alert("No valid alerts to save.");
      return;
    }

    setRtuAlerts(alertKey, alerts);
    saveConfig();

    const tick = document.getElementById("em-tick");
    if (tick) {
      tick.style.display = "inline";
      setTimeout(() => (tick.style.display = "none"), 1200);
    }
  };



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
      return `<button class="plc-brand" data-brand="${bk}" style="${tabBtnStyle(active)}"
        onmouseenter="if(!this.classList.contains('active')) this.style.background='#f3f4f6'"
        onmouseleave="if(!this.classList.contains('active')) this.style.background='#fff'"
      >${bk}</button>`;
    })
    .join("");

  const slaveTabs = (map[selectedBrand] || [])
    .map((sid) => {
      const active = sid === selectedSlave;
      return `<button class="plc-slave" data-slave="${sid}" style="${tabBtnStyle(active)}"
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
        <td style="${tdStyle}"><input type="number" step="any" name="threshold_${i}" value="${row.threshold ?? ""}" style="${inputStyle("120px")}"></td>
        <td style="${tdStyle}"><input type="text" name="contact_${i}" value="${row.contact ?? ""}" style="${inputStyle("180px")}"></td>
        <td style="${tdStyle}"><input type="text" name="message_${i}" value="${row.message ?? ""}" style="${inputStyle("220px")}"></td>
        <td style="${tdStyle}; text-align:center;"><input type="checkbox" name="enabled_${i}" ${row.enabled ? "checked" : ""}></td>
        <td style="${tdStyle}; text-align:center;">
          <button type="button" class="plc-del" data-idx="${i}" style="border:1px solid #ef4444; color:#ef4444; background:#fff; padding:6px 10px; border-radius:6px; font-size:12px; cursor:pointer;">Remove</button>
        </td>
      </tr>
    `,
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
              ${rows || `<tr><td colspan="9" style="padding:14px; color:${UI_MUTED}; text-align:center;">No alerts for <span style="color:${UI_TEXT}; font-weight:600;">${brandKey}</span> · <span style="color:${UI_TEXT}; font-weight:600;">Slave ${slaveId}</span>. Click “Add Alert”.</td></tr>`}
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
        } catch (e) { }
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
          } catch (e) { }
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

function renderNetworkPanel() {
  const APN_MAP = {
    jio: "jionet",
    airtel: "airtelgprs.com",
    bsnl: "bsnlnet",
    vi: "www",
    other: "",
  };

  const $ = (id) => document.getElementById(id);

  // ---------- PANEL GUARD ----------
  const panel = $("main-panel");
  if (!panel) return;

  // ---------- HARD GUARANTEES ----------
  if (!config.network) config.network = {};
  if (!config.network.wifi) config.network.wifi = { ssid: "", password: "" };
  if (!config.network.sim4g) config.network.sim4g = { provider: "", apn: "" };

  if (!config.network.static) {
    config.network.static = {
      iface: "eth0",
      enabled: false,
      ip: "",
      subnet: "",
      gateway: "",
      dns_primary: "",
      dns_secondary: "",
    };
  }

  if (!config.network.static2) {
    config.network.static2 = {
      iface: "eth1",
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
  panel.innerHTML = `
    <div class="panel-header">Network Configuration</div>

    <div class="tab-list">
      <button class="active" data-tab="wifi">Wi-Fi</button>
      <button data-tab="sim">4G SIM</button>
      <button data-tab="static">Ethernet 1</button>
      <button data-tab="static2">Ethernet 2</button>
    </div>

    <!-- Wi-Fi -->
    <div class="tab-content" id="wifi-tab">
      <fieldset class="net-fieldset">
        <label>SSID</label>
        <input id="wifi-ssid" value="${net.wifi.ssid}">
        <label>Password</label>
        <input id="wifi-password" type="password" value="${net.wifi.password}">
      </fieldset>
      <button class="button-primary" id="save-wifi">Save Wi-Fi</button>
      <span class="checkmark" id="wifi-tick" style="display:none">✔</span>
    </div>

    <!-- SIM -->
    <div class="tab-content" id="sim-tab" style="display:none">
      <fieldset class="net-fieldset">
        <label>Provider</label>
        <select id="apn-provider">
          <option value="">Select provider</option>
          ${Object.keys(APN_MAP).map(
    p => `<option value="${p}" ${net.sim4g.provider === p ? "selected" : ""}>${p.toUpperCase()}</option>`
  ).join("")}
        </select>
        <label>APN</label>
        <input id="apn-value" value="${net.sim4g.apn}">
      </fieldset>
      <button class="button-primary" id="save-sim">Save SIM</button>
      <span class="checkmark" id="sim-tick" style="display:none">✔</span>
    </div>

<!-- Ethernet 1 -->
<div class="tab-content" id="static-tab" style="display:none">
  <fieldset class="net-fieldset">
    <label>
      <input type="checkbox" id="static1-enable">
      Enable Static IP
    </label>

    <label for="static1-ip">IP Address</label>
    <input id="static1-ip" value="${net.static.ip}">

    <label for="static1-subnet">Subnet Mask</label>
    <input id="static1-subnet" value="${net.static.subnet}">

    <label for="static1-gw">Gateway</label>
    <input id="static1-gw" value="${net.static.gateway}">

    <label for="static1-dns1">Primary DNS</label>
    <input id="static1-dns1" value="${net.static.dns_primary}">

    <label for="static1-dns2">Secondary DNS</label>
    <input id="static1-dns2" value="${net.static.dns_secondary}">
  </fieldset>

  <button class="button-primary" id="save-static">Save Static IP</button>
  <span class="checkmark" id="static1-tick" style="display:none">✔</span>
</div>


<!-- Ethernet 2 -->
<div class="tab-content" id="static2-tab" style="display:none">
  <fieldset class="net-fieldset">
    <label>
      <input type="checkbox" id="static2-enable">
      Enable Static IP
    </label>

    <label for="static2-ip">IP Address</label>
    <input id="static2-ip" value="${net.static2.ip}">

    <label for="static2-subnet">Subnet Mask</label>
    <input id="static2-subnet" value="${net.static2.subnet}">

    <label for="static2-gw">Gateway</label>
    <input id="static2-gw" value="${net.static2.gateway}">

    <label for="static2-dns1">Primary DNS</label>
    <input id="static2-dns1" value="${net.static2.dns_primary}">

    <label for="static2-dns2">Secondary DNS</label>
    <input id="static2-dns2" value="${net.static2.dns_secondary}">
  </fieldset>

  <button class="button-primary" id="save-static2">Save Static IP</button>
  <span class="checkmark" id="static2-tick" style="display:none">✔</span>
</div>

  `;

  // ---------- TABS ----------
  document.querySelectorAll(".tab-list button").forEach(btn => {
    btn.onclick = () => {
      document.querySelectorAll(".tab-list button")
        .forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      ["wifi", "sim", "static", "static2"].forEach(t => {
        $(`${t}-tab`).style.display =
          btn.dataset.tab === t ? "block" : "none";
      });
    };
  });

  // ---------- APN ----------
  const providerSel = $("apn-provider");
  const apnInput = $("apn-value");

  providerSel.onchange = () => {
    if (!providerSel.value) {
      apnInput.value = "";
      apnInput.disabled = true;
    } else if (providerSel.value === "other") {
      apnInput.disabled = false;
      apnInput.value = "";
    } else {
      apnInput.value = APN_MAP[providerSel.value];
      apnInput.disabled = true;
    }
  };
  providerSel.onchange();

  // ---------- STATIC TOGGLES ----------
  function toggle(prefix) {
    const en = $(`${prefix}-enable`).checked;
    ["ip", "subnet", "gw", "dns1", "dns2"]
      .forEach(f => $(`${prefix}-${f}`).disabled = !en);
  }

  $("static1-enable").checked = net.static.enabled;
  $("static2-enable").checked = net.static2.enabled;

  toggle("static1");
  toggle("static2");

  $("static1-enable").onchange = () => toggle("static1");
  $("static2-enable").onchange = () => toggle("static2");

  // ---------- SAVE ----------
  $("save-wifi").onclick = async () => {
    net.wifi.ssid = $("wifi-ssid").value.trim();
    net.wifi.password = $("wifi-password").value;
    await saveConfig();
    showTick("wifi-tick");
  };

  $("save-sim").onclick = async () => {
    net.sim4g.provider = providerSel.value;
    net.sim4g.apn = apnInput.value.trim();
    await saveConfig();
    showTick("sim-tick");
  };

  $("save-static").onclick = async () => {
    Object.assign(net.static, {
      enabled: $("static1-enable").checked,
      ip: $("static1-ip").value.trim(),
      subnet: $("static1-subnet").value.trim(),
      gateway: $("static1-gw").value.trim(),
      dns_primary: $("static1-dns1").value.trim(),
      dns_secondary: $("static1-dns2").value.trim(),
    });
    await saveConfig();
    showTick("static1-tick");
  };

  $("save-static2").onclick = async () => {
    Object.assign(net.static2, {
      enabled: $("static2-enable").checked,
      ip: $("static2-ip").value.trim(),
      subnet: $("static2-subnet").value.trim(),
      gateway: $("static2-gw").value.trim(),
      dns_primary: $("static2-dns1").value.trim(),
      dns_secondary: $("static2-dns2").value.trim(),
    });
    await saveConfig();
    showTick("static2-tick");
  };
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
              <label><input type="radio" name="enabled" value="true" ${offlineCfg.enabled ? "checked" : ""
    }> Enable</label>
              <label><input type="radio" name="enabled" value="false" ${!offlineCfg.enabled ? "checked" : ""
    }> Disable</label>
            </div>
            <div class="section">
              <legend>Mode</legend>
              <label><input type="radio" name="mode" value="live" ${offlineCfg.mode === "live" ? "checked" : ""
    }> Live</label>
              <label><input type="radio" name="mode" value="schedule" ${offlineCfg.mode === "schedule" ? "checked" : ""
    }> Schedule</label>
            </div>
            <section class="ftp-schedule">
            <div class="section">
              <legend>FTP Settings</legend>
              <label>FTP Server IP: <input type="text" name="ftpserver" value="${offlineCfg.ftp.server
    }"></label>
              <label>Username: <input type="text" name="ftpuser" value="${offlineCfg.ftp.user
    }"></label>
              <label>Password: <input type="password" name="ftppass" value="${offlineCfg.ftp.pass
    }"></label>
              <label>Port Number: <input type="number" name="ftpport" value="${offlineCfg.ftp.port
    }"></label>
              <label>Backup Folder: <input type="text" name="ftpfolder" value="${offlineCfg.ftp.folder
    }"></label>
            </div>
            <div class="section" id="schedule-section">
              <legend>Schedule</legend>
              <div>
                Time: h:
                <input type="number" name="hour" min="0" max="23" value="${offlineCfg.schedule.hour
    }" style="width: 50px;">
                m:
                <input type="number" name="min" min="0" max="59" value="${offlineCfg.schedule.min
    }" style="width: 50px;">
                s:
                <input type="number" name="sec" min="0" max="59" value="${offlineCfg.schedule.sec
    }" style="width: 50px;">
                (24 Hr Format)
              </div>
              <div style="margin-top: 12px;">
                Date: <input type="date" name="date" value="${offlineCfg.schedule.date
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
    radio.addEventListener("change", updateScheduleVisibility),
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
        "0",
      )}:${form.min.value.padStart(2, "0")}:${form.sec.value.padStart(2, "0")}`,
    );

    // --- Build min allowed date+time from offlineCfg ---
    const cfg = offlineCfg.schedule;
    const minAllowedDateTime = new Date(
      `${cfg.date}T${String(cfg.hour).padStart(2, "0")}:${String(
        cfg.min,
      ).padStart(2, "0")}:${String(cfg.sec).padStart(2, "0")}`,
    );

    // --- Compare ---
    if (selectedDateTime < minAllowedDateTime) {
      alert(
        "Selected date/time cannot be earlier than the configured schedule!",
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
        .file-content, .columns-container {
          scrollbar-width: thin;
          scrollbar-color: #007bff #f1f1f1;
          box-sizing: border-box;
          scrollbar-gutter: stable;
        }
        .file-content::-webkit-scrollbar,
        .columns-container::-webkit-scrollbar {
          width: 8px;
        }
        .file-content::-webkit-scrollbar-track,
        .columns-container::-webkit-scrollbar-track {
          background: #f1f1f1;
          border-radius: 4px;
        }
        .file-content::-webkit-scrollbar-thumb,
        .columns-container::-webkit-scrollbar-thumb {
          background: #007bff;
          border-radius: 4px;
        }
        .file-content::-webkit-scrollbar-thumb:hover,
        .columns-container::-webkit-scrollbar-thumb:hover {
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
        <span id="process-tick" style="display:none;font-size:18px;color:#49ba3c;">&#10004;</span>
      </div>
    `;

  // Helpers
  function addColumnToFetch(containerId, columnName = "") {
    const container = document.getElementById(containerId);
    if (!container) {
      console.error(`Container with id ${containerId} not found`);
      return;
    }
    const columnId = `column-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const columnHtml = `
        <div class="column-field" id="${columnId}" style="display:flex;align-items:center;gap:8px;margin-bottom:5px;padding:5px 8px;background:#f8f9fa;border-radius:4px;border:1px solid #e9ecef;">
          <input type="text" class="column-name" value="${columnName}" placeholder="Column name" style="flex:1;padding:4px 8px;border:1px solid #ddd;border-radius:3px;font-size:14px;min-width:120px;">
          <button type="button" class="remove-column-btn" style="background:#dc3545;color:white;border:none;padding:3px 8px;border-radius:3px;cursor:pointer;font-size:12px;white-space:nowrap;">×</button>
        </div>
      `;
    container.insertAdjacentHTML("beforeend", columnHtml);
    const removeBtn = document
      .getElementById(columnId)
      ?.querySelector(".remove-column-btn");
    if (removeBtn) {
      removeBtn.onclick = () => {
        document.getElementById(columnId).remove();
      };
    }
  }

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

    const fileLocation = configData?.SMBShare?.local_mount_point || "";
    const fileType = configData?.file_details?.file_type || "json";
    const skipLines = configData?._internal?.skipLines || 0;
    const dataFreq = configData?.["data_freq(in secs)"] ?? "";
    const columnsToFetch = configData?.file_details?.columns_to_fetch || [];
    const hasDatetime = configData?._internal?.hasDatetime || false;
    const datetimeInfo = configData?._internal?.datetimeInfo || null;
    const filterField = configData?._internal?.filterField || "";
    const fileKeyword = configData?.file_details?.file_keyword || "";
    const logFilePath = configData?.log_file_path || "";

    const smbShare = configData?.SMBShare?.smb_share || "";
    const sudoPassword = configData?.SMBShare?.sudo_password || "";
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
                <input type="checkbox" class="file-enabled" ${enabled ? "checked" : ""} style="opacity:0;width:0;height:0;">
                <span class="slider" style="position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background-color:#ccc;transition:.4s;border-radius:24px;"></span>
              </label>
            </div>
          </div>

          <div class="file-content" style="max-height:${collapsed ? "0px" : "500px"};overflow-y:auto;overflow-x:hidden;padding:${collapsed ? "0 15px" : "0 15px 15px 15px"};opacity:${collapsed ? "0" : "1"};transition:all 0.3s ease;">
            <div class="file-form">
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px;">
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">File Location (Local Mount Point):</label>
                  <input type="text" class="file-location" placeholder="D:/path/to/local/mount" value="${fileLocation}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                </div>
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">File Type:</label>
                  <select class="file-type" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                    <option value="json" ${fileType === "json" ? "selected" : ""}>JSON</option>
                    <option value="csv" ${fileType === "csv" ? "selected" : ""}>CSV</option>
                    <option value="excel" ${fileType === "excel" ? "selected" : ""}>Excel</option>
                  </select>
                </div>
              </div>

              <!-- SMB Details -->
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px;">
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">SMB Share:</label>
                  <input type="text" class="smb-share" placeholder="//server/share" value="${smbShare}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                </div>
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">Share Username:</label>
                  <input type="text" class="share-username" placeholder="username" value="${shareUsername}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                </div>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px;">
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">Share Password:</label>
                  <input type="password" class="share-password" placeholder="password" value="${sharePassword}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                </div>
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">Sudo Password:</label>
                  <input type="password" class="sudo-password" placeholder="sudo password" value="${sudoPassword}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                </div>
              </div>

              <!-- Skip lines + Data Frequency + File Keyword on the same row -->
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:15px;">
                <div class="skip-lines-container" style="display:${fileType === "csv" || fileType === "excel" ? "block" : "none"};">
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">Number of lines to skip:</label>
                  <input type="number" class="skip-lines" value="${skipLines}" min="0" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                </div>
                <div>
                  <label style="display:block;margin-bottom:5px;font-weight:bold;">Data Frequency (in secs) & File Keyword:</label>
                  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                    <input type="number" class="data-freq" placeholder="60" value="${dataFreq}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                    <input type="text" class="file-keyword" placeholder="e.g., hpml_data" value="${fileKeyword}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                  </div>
                </div>
              </div>

              <div style="margin-bottom:15px;">
                <label style="display:block;margin-bottom:5px;font-weight:bold;">Log File Path:</label>
                <input type="text" class="log-file-path" placeholder="D:/path/to/logs" value="${logFilePath}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
              </div>

              <div style="margin-bottom:15px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                  <label style="font-weight:bold;">Columns to Fetch:</label>
                  <button type="button" class="add-column-btn" style="background:#28a745;color:white;border:none;padding:4px 12px;border-radius:3px;cursor:pointer;font-size:13px;">+ Add</button>
                </div>
                <div class="columns-container" id="columns-${fileCount}" style="max-height:150px;overflow-y:auto;overflow-x:hidden;border:1px solid #e9ecef;border-radius:4px;padding:8px;background:white;">
                  <!-- Column fields will be added here -->
                </div>
              </div>

              <div style="margin-bottom:15px;">
                <label style="display:flex;align-items:center;gap:10px;font-weight:bold;">
                  <input type="checkbox" class="has-datetime" ${hasDatetime ? "checked" : ""}>
                  Has DateTime/Date fields
                </label>
                <div class="datetime-options" style="display:${hasDatetime ? "block" : "none"};margin-top:10px;padding:15px;background:#e9ecef;border-radius:4px;">
                  <div style="margin-bottom:15px;">
                    <label style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
                      <input type="radio" name="datetime-type-${fileCount}" value="combined" ${!datetimeInfo || datetimeInfo?.type === "combined" ? "checked" : ""}>
                      Combined DateTime
                    </label>
                    <label style="display:flex;align-items:center;gap:10px;">
                      <input type="radio" name="datetime-type-${fileCount}" value="separate" ${datetimeInfo?.type === "separate" ? "checked" : ""}>
                      Separate Date and Time fields
                    </label>
                  </div>
                  <div class="datetime-fields">
                    <div class="combined-datetime-field" style="margin-bottom:10px;display:${!datetimeInfo || datetimeInfo?.type === "combined" ? "block" : "none"};">
                      <label style="display:block;margin-bottom:5px;font-weight:bold;">DateTime Column Name:</label>
                      <input type="text" class="datetime-column" placeholder="Enter datetime column name" value="${datetimeInfo?.datetimeColumn || ""}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                    </div>
                    <div class="separate-datetime-fields" style="display:${datetimeInfo?.type === "separate" ? "block" : "none"};">
                      <div style="display:grid;grid-template-columns:1fr 1fr;gap:15px;">
                        <div>
                          <label style="display:block;margin-bottom:5px;font-weight:bold;">Date Column Name:</label>
                          <input type="text" class="date-column" placeholder="Enter date column name" value="${datetimeInfo?.dateColumn || ""}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                        </div>
                        <div>
                          <label style="display:block;margin-bottom:5px;font-weight:bold;">Time Column Name:</label>
                          <input type="text" class="time-column" placeholder="Enter time column name" value="${datetimeInfo?.timeColumn || ""}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div class="filter-field-container" style="margin-bottom:15px;display:${hasDatetime ? "none" : "block"};">
                <label style="display:block;margin-bottom:5px;font-weight:bold;">Filter Field (for duplicate detection):</label>
                <input type="text" class="filter-field" placeholder="Field name to filter duplicates" value="${filterField}" style="width:100%;padding:8px;border:1px solid #ddd;border-radius:4px;">
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

    // Populate columns
    if (columnsToFetch && columnsToFetch.length > 0) {
      columnsToFetch.forEach((columnName) => {
        addColumnToFetch(`columns-${fileCount}`, columnName);
      });
    } else {
      addColumnToFetch(`columns-${fileCount}`);
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

    // File type change handler
    const fileTypeSelect = fileConfig.querySelector(".file-type");
    if (fileTypeSelect) {
      fileTypeSelect.onchange = (e) => {
        const skipContainer = fileConfig.querySelector(".skip-lines-container");
        if (skipContainer) {
          if (e.target.value === "csv" || e.target.value === "excel") {
            skipContainer.style.display = "block";
          } else {
            skipContainer.style.display = "none";
          }
        }
      };
    }

    // DateTime checkbox handler
    const hasDatetimeCheckbox = fileConfig.querySelector(".has-datetime");
    if (hasDatetimeCheckbox) {
      hasDatetimeCheckbox.onchange = (e) => {
        const datetimeOptions = fileConfig.querySelector(".datetime-options");
        const filterFieldContainer = fileConfig.querySelector(
          ".filter-field-container",
        );
        if (e.target.checked) {
          if (datetimeOptions) datetimeOptions.style.display = "block";
          if (filterFieldContainer) filterFieldContainer.style.display = "none";
          const checkedRadio = fileConfig.querySelector(
            `input[name="datetime-type-${fileCount}"]:checked`,
          );
          if (checkedRadio) checkedRadio.dispatchEvent(new Event("change"));
        } else {
          if (datetimeOptions) datetimeOptions.style.display = "none";
          if (filterFieldContainer)
            filterFieldContainer.style.display = "block";
        }
      };
    }

    fileConfig
      .querySelectorAll(`input[name="datetime-type-${fileCount}"]`)
      .forEach((radio) => {
        radio.onchange = (e) => {
          const combinedField = fileConfig.querySelector(
            ".combined-datetime-field",
          );
          const separateFields = fileConfig.querySelector(
            ".separate-datetime-fields",
          );
          if (e.target.value === "combined") {
            if (combinedField) combinedField.style.display = "block";
            if (separateFields) separateFields.style.display = "none";
          } else if (e.target.value === "separate") {
            if (combinedField) combinedField.style.display = "none";
            if (separateFields) separateFields.style.display = "block";
          }
        };
      });

    // Add column button
    const addColumnBtn = fileConfig.querySelector(".add-column-btn");
    if (addColumnBtn) {
      addColumnBtn.onclick = () => {
        addColumnToFetch(`columns-${fileCount}`);
      };
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

  // Save configuration (no file_name, file_keyword next to data freq)
  function saveCurrentConfigurationState() {
    const fileConfigs = document.querySelectorAll(".file-config");
    const filesToProcess = [];
    fileConfigs.forEach((configElement, index) => {
      const content = configElement.querySelector(".file-content");
      const isCollapsed =
        content &&
        (content.style.maxHeight === "0px" || content.style.opacity === "0");
      const enabled =
        configElement.querySelector(".file-enabled")?.checked || false;

      const columnsToFetch = [];
      configElement.querySelectorAll(".column-field").forEach((field) => {
        const columnName = field.querySelector(".column-name")?.value?.trim();
        if (columnName) columnsToFetch.push(columnName);
      });

      const hasDatetime =
        configElement.querySelector(".has-datetime")?.checked || false;
      let datetimeInfo = null;
      if (hasDatetime) {
        const datetimeType = configElement.querySelector(
          `input[name="datetime-type-${index + 1}"]:checked`,
        )?.value;
        if (datetimeType === "combined") {
          datetimeInfo = {
            type: "combined",
            datetimeColumn:
              configElement.querySelector(".datetime-column")?.value || "",
          };
        } else {
          datetimeInfo = {
            type: "separate",
            dateColumn:
              configElement.querySelector(".date-column")?.value || "",
            timeColumn:
              configElement.querySelector(".time-column")?.value || "",
          };
        }
      }

      const fileLocation =
        configElement.querySelector(".file-location")?.value || "";
      const fileKeyword =
        configElement.querySelector(".file-keyword")?.value || "";
      const logFilePath =
        configElement.querySelector(".log-file-path")?.value || "";

      const smbShare = configElement.querySelector(".smb-share")?.value || "";
      const sudoPassword =
        configElement.querySelector(".sudo-password")?.value || "";
      const shareUsername =
        configElement.querySelector(".share-username")?.value || "";
      const sharePassword =
        configElement.querySelector(".share-password")?.value || "";

      const dataFreqVal = parseInt(
        configElement.querySelector(".data-freq")?.value,
      );
      const dataFreqSecs = Number.isFinite(dataFreqVal) ? dataFreqVal : 60;

      const configData = {
        SMBShare: {
          smb_share: smbShare,
          local_mount_point: fileLocation,
          sudo_password: sudoPassword,
          share_username: shareUsername,
          share_password: sharePassword,
        },
        file_details: {
          file_type: configElement.querySelector(".file-type")?.value || "json",
          file_header: null,
          columns_to_fetch: columnsToFetch,
          file_keyword: fileKeyword,
        },
        // No DB credentials or schema; no file_name mapping anymore
        storing_database: {
          table_name: fileKeyword, // intentionally left blank as per instruction (no file_name)
        },
        processed_files_table: "", // left blank since no table name
        log_file_path: logFilePath,
        "data_freq(in secs)": dataFreqSecs,
        _internal: {
          enabled: enabled,
          collapsed: isCollapsed,
          skipLines:
            parseInt(configElement.querySelector(".skip-lines")?.value) || 0,
          hasDatetime: hasDatetime,
          datetimeInfo: datetimeInfo,
          filterField:
            configElement.querySelector(".filter-field")?.value || "",
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
    ",",
  )}">
        </label><br><br>

        <label>Topic:<br>
          <input type="text" name="topic" style="width:100%" value="${kafkaConfig.topic
    }">
        </label><br><br>
  <fieldset>
    <legend>Certificates</legend>

    <label>CA Certificate:</label>
    ${kafkaConfig.certFiles.ca
      ? `<div>
            <small>${kafkaConfig.certFiles.ca}</small>
            <button type="button" class="change-btn" data-field="ca">Change</button>
          </div>`
      : `<input type="file" name="ca" id="caFile">`
    }
    <br>

    <label>Client Certificate:</label>
    ${kafkaConfig.certFiles.cert
      ? `<div>
            <small>${kafkaConfig.certFiles.cert}</small>
            <button type="button" class="change-btn" data-field="cert">Change</button>
          </div>`
      : `<input type="file" name="cert" id="certFile">`
    }
    <br>

    <label>Client Key:</label>
    ${kafkaConfig.certFiles.key
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

function renderDatabasePanel() {
  const db = config.Database;

  function renderBlock(name, obj) {
    return `
      <div class="db-block">
        <h3>${name.toUpperCase()} Database</h3>

        <label>
          <input type="checkbox" id="${name}-enabled" ${obj.enabled ? "checked" : ""}>
          Enabled
        </label>

        <div class="form-grid">
          ${Object.entries(obj.cred).map(
      ([key, val]) => `
              <label>${key}</label>
              <input
                type="${key === "password" ? "password" : "text"}"
                id="${name}-${key}"
                value="${val}">
            `
    ).join("")}
        </div>
      </div>
    `;
  }

  document.getElementById("main-panel").innerHTML = `
    <div class="panel-header">Database Configuration</div>
    <div class="tab-content">
    ${renderBlock("local", db.local)}
    ${renderBlock("cloud", db.cloud)}
    <button class="primary-btn" onclick="saveDatabaseConfig()">Save</button>
    </div>
  `;
}


function saveDatabaseConfig() {
  const DATABASE = config.Database;

  ["local", "cloud"].forEach(name => {
    DATABASE[name].enabled =
      document.getElementById(`${name}-enabled`).checked;

    Object.keys(DATABASE[name].cred).forEach(key => {
      const el = document.getElementById(`${name}-${key}`);
      if (!el) return;

      DATABASE[name].cred[key] =
        key === "port" ? Number(el.value) : el.value;
    });
  });

  saveConfig();
}

function renderAdminSettingsPanel() {
  if (!config.adminSettings) {
    config.adminSettings = {
      mailBody: {
        analog: "",
        digital: "",
        modbus_rtu: "",
        plc: ""
      }
    };
  }

  if (!Array.isArray(config.adminSettings.rs485Ports)) {
    config.adminSettings.rs485Ports = [];
  }
  if (!Array.isArray(config.adminSettings.engineeringUnits)) {
    config.adminSettings.engineeringUnits = [
      { type: "temperature", symbols: ["°C", "°F"] },
      { type: "flow", symbols: ["L/min", "m³/h"] },
      { type: "pressure", symbols: ["bar", "psi", "Pa"] },
      { type: "level", symbols: ["%", "m"] },
      { type: "energy", symbols: ["kWh", "Wh"] },
      { type: "voltage", symbols: ["V"] },
      { type: "current", symbols: ["A", "mA"] }
    ];
  }




  document.getElementById("main-panel").innerHTML = `
    <div style="
      font-family: Segoe UI, Arial;
      background:#f4f6f8;
      padding:20px;
      height:100%;
    ">

      <div style="
        font-size:20px;
        font-weight:600;
        border-bottom:2px solid #cfd4da;
        padding-bottom:8px;
        margin-bottom:15px;
      ">
        Admin Settings
      </div>

      <div style="display:flex; gap:8px; margin-bottom:12px;">
      ${["analog", "digital", "modbus_rtu", "plc", "smtp"].map(t => `
        <button onclick="renderAdminTab('${t}')"
          style="
            padding:8px 14px;
            border:1px solid #c5c9ce;
            background:#e8ebef;
            cursor:pointer;
            border-radius:4px;
          ">
          ${t.replace("_", " ").toUpperCase()}
        </button>
      `).join("")}
      
      </div>

      <div id="admin-tab-content"
        style="
          background:#fff;
          border:1px solid #c5c9ce;
          padding:16px;
          border-radius:4px;
        ">
      </div>

      <div style="margin-top:15px; text-align:right;">
        <button onclick="saveConfig()"
          style="
            padding:8px 18px;
            background:#1e88e5;
            color:#fff;
            border:none;
            border-radius:4px;
            cursor:pointer;
            font-weight:500;
          ">
          Save
        </button>
      </div>
    </div>
  `;

  renderAdminTab("analog");
}

function addEngineeringUnit() {
  config.adminSettings.engineeringUnits.push({
    type: "",
    symbols: []
  });
  renderModbusSubTab("engineering");
}

function removeEngineeringUnit(i) {
  config.adminSettings.engineeringUnits.splice(i, 1);
  renderModbusSubTab("engineering");
}


function renderModbusSubTab(sub) {
  const el = document.getElementById("modbus-subtab-content");

  if (sub === "rs485") {
    const ports = config.adminSettings.rs485Ports;

    el.innerHTML = `
      <div style="margin-bottom:10px; font-weight:600;">
        RS485 Ports
      </div>

      <div id="rs485-list">
        ${ports.map((p, i) => `
<div style="
  display:grid;
  grid-template-columns:150px 1fr auto;
  gap:8px;
  margin-bottom:6px;
  align-items:center;
">
  <label>Port Name</label>
  <input
    value="${p.name}"
    onchange="updateRS485(${i}, 'name', this.value)"
  >
  <button onclick="removeRS485(${i})">✖</button>

  <label>Device</label>
  <input
    value="${p.port}"
    onchange="updateRS485(${i}, 'port', this.value)"
  >
  <span></span>
</div>

        `).join("")}
      </div>

      <button onclick="addRS485()" style="margin-top:8px;">
        + Add RS485 Port
      </button>
    `;
  }

  if (sub === "mail") {
    const key = "modbus_rtu";
    el.innerHTML = `
    <div style="margin-top:15px;">
      <label style="display:block; font-weight:600; margin-bottom:6px;">
        Mail Body
      </label>
      <textarea rows="5"
        style="
          width:100%;
          border:1px solid #c5c9ce;
          padding:8px;
          font-family: monospace;
          border-radius:4px;
        "
        onchange="config.adminSettings.mailBody['${key}'] = this.value"
      >${config.adminSettings.mailBody[key] || ""}</textarea>
    </div>
    `;
  }
  if (sub === "random") {
    const brands = config.ModbusRTU?.Devices?.brands || {};
    let html = "";

    Object.entries(brands).forEach(([brand, data]) => {
      data.slaves.forEach((slave, idx) => {
        html += `
          <label style="display:block; margin-bottom:6px;">
            <input type="checkbox"
              ${slave.generate_random ? "checked" : ""}
              onchange="
                config.ModbusRTU.Devices.brands['${brand}']
                .slaves[${idx}].generate_random = this.checked
              ">
            ${data.label} – Slave ${slave.id} (Random)
          </label>
        `;
      });
    });

    el.innerHTML = `
      <div style="font-weight:600; margin-bottom:10px;">
        Modbus RTU – Random Value Control
      </div>
      ${html || "<i>No Modbus slaves configured</i>"}
    `;
  }

  if (sub === "engineering") {
    const list = config.adminSettings.engineeringUnits;

    el.innerHTML = `
      <div style="font-weight:600; margin-bottom:10px;">
        Engineering Units (Sensor → Symbols)
      </div>
  
      ${list.map((row, i) => `
        <div style="
          display:grid;
          grid-template-columns:180px 1fr auto;
          gap:8px;
          margin-bottom:6px;
          align-items:center;
        ">
          <input
            placeholder="Sensor Type (e.g. temperature)"
            value="${row.type}"
            onchange="config.adminSettings.engineeringUnits[${i}].type = this.value"
          >
  
          <input
            placeholder="Symbols (comma separated, e.g. °C,°F)"
            value="${row.symbols.join(',')}"
            onchange="
              config.adminSettings.engineeringUnits[${i}].symbols =
                this.value.split(',').map(s => s.trim()).filter(Boolean)
            "
          >
  
          <button onclick="removeEngineeringUnit(${i})">✖</button>
        </div>
      `).join("")}
  
      <button onclick="addEngineeringUnit()" style="margin-top:8px;">
        + Add Sensor Type
      </button>
    `;
  }

}

function addRS485() {
  config.adminSettings.rs485Ports.push({
    name: "",
    port: ""
  });
  renderModbusSubTab("rs485");
}

function removeRS485(index) {
  config.adminSettings.rs485Ports.splice(index, 1);
  renderModbusSubTab("rs485");
}

function updateRS485(index, field, value) {
  config.adminSettings.rs485Ports[index][field] = value;
}


function renderAdminTab(tab) {
  const container = document.getElementById("admin-tab-content");

  if (tab === "analog") {
    const analog = config.ioSettings.analog;
    container.innerHTML = `
      <h3>Analog</h3>

      <label>
        <input type="checkbox"
          ${analog.generate_random ? "checked" : ""}
          onchange="config.ioSettings.analog.generate_random = this.checked">
        Generate Random Values
      </label>

      ${renderMailBody("analog")}
    `;
  }

  if (tab === "digital") {
    const di = config.ioSettings.digitalInput;
    container.innerHTML = `
      <h3>Digital</h3>

      <label>
        <input type="checkbox"
          ${di.generate_random ? "checked" : ""}
          onchange="config.ioSettings.digitalInput.generate_random = this.checked">
        Generate Random Values
      </label>

      ${renderMailBody("digital")}
    `;
  }

  if (tab === "modbus_rtu") {
    container.innerHTML = `
      <div style="margin-bottom:12px;">
        <button onclick="renderModbusSubTab('mail')">MAIL BODY</button>
        <button onclick="renderModbusRTURandom()">Generate Random Values</button>
        <button onclick="renderModbusSubTab('rs485')">RS485</button>
        <button onclick="renderModbusSubTab('engineering')">ENGINEERING UNITS</button>
      </div>
  
      <div id="modbus-subtab-content"></div>
    `;

    renderModbusSubTab("mail");
  }

  if (tab === "plc") {
    container.innerHTML = `
      <h3>PLC Configuration</h3>
      ${renderPLCRandom()}
      ${renderMailBody("plc")}
    `;
  }

  if (tab === "smtp") {
    const s = config.smtp;

    container.innerHTML = `
    <h3 style="
      margin-top:0;
      border-bottom:1px solid #ddd;
      padding-bottom:6px;
    ">
      SMTP Configuration
    </h3>

    <div style="
      display:grid;
      grid-template-columns:150px 1fr;
      gap:10px;
      margin-top:12px;
    ">
      <label>Server</label>
      <input value="${s.server}"
        onchange="config.smtp.server = this.value"
        style="padding:6px; border:1px solid #c5c9ce;">

      <label>Port</label>
      <input type="number" value="${s.port}"
        onchange="config.smtp.port = Number(this.value)"
        style="padding:6px; border:1px solid #c5c9ce;">

      <label>User</label>
      <input value="${s.user}"
        onchange="config.smtp.user = this.value"
        style="padding:6px; border:1px solid #c5c9ce;">

      <label>Password</label>
      <input type="password" value="${s.password}"
        onchange="config.smtp.password = this.value"
        style="padding:6px; border:1px solid #c5c9ce;">
    </div>

    <p style="margin-top:12px; color:#666; font-size:13px;">
      SMTP settings are transport-level configuration only.
    </p>
  `;
    return;
  }


}

function renderMailBody(key) {
  return `
    <div style="margin-top:15px;">
      <label style="display:block; font-weight:600; margin-bottom:6px;">
        Mail Body
      </label>
      <textarea rows="5"
        style="
          width:100%;
          border:1px solid #c5c9ce;
          padding:8px;
          font-family: monospace;
          border-radius:4px;
        "
        onchange="config.adminSettings.mailBody['${key}'] = this.value"
      >${config.adminSettings.mailBody[key] || ""}</textarea>
    </div>
  `;
}

function renderModbusRTURandom1() {
  const brands = config.ModbusRTU.Devices.brands;
  let html = "";

  Object.entries(brands).forEach(([brand, data]) => {
    data.slaves.forEach((slave, idx) => {
      html += `
        <label>
          <input type="checkbox"
            ${slave.generate_random ? "checked" : ""}
            onchange="config.ModbusRTU.Devices.brands['${brand}'].slaves[${idx}].generate_random = this.checked">
          ${data.label} - Slave ${slave.id} (Random)
        </label><br>
      `;
    });
  });

  return html;
}

function renderModbusRTURandom() {
  renderModbusSubTab("random");
}


function renderPLCRandom() {
  return config.plc_configurations.map((plc, i) => `
    <label>
      <input type="checkbox"
        ${plc.generate_random ? "checked" : ""}
        onchange="config.plc_configurations[${i}].generate_random = this.checked">
      ${plc.plcType} PLC (Random)
    </label><br>
  `).join("");
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
        1200,
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

function lockViewOnly(container = document) {
  if (!isReadOnly()) return;

  container.querySelectorAll(
    "input, select, textarea, button"
  ).forEach(el => {
    // allow side navigation
    if (el.closest("#side-nav")) return;

    // allow logout & help
    if (el.id === "logout" || el.dataset.allow === "true") return;

    el.disabled = true;
    el.readOnly = true;
    el.style.pointerEvents = "none";
    el.style.opacity = "0.6";
  });
}


function applyRoleBasedMenu(role) {
  const adminItems = [
    document.getElementById("nav-database"),
    document.getElementById("nav-admin-settings"),
    // document.getElementById("nav-alarms")
  ];

  if (role === "admin" || role === "superadmin") {
    adminItems.forEach(el => el && (el.style.display = "block"));
  } else {
    adminItems.forEach(el => el && (el.style.display = "none"));
  }
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
    else if (panel === "database") {
      renderDatabasePanel();
    }
    else if (panel === "admin-settings") renderAdminSettingsPanel();


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
        "For help, contact support.<br>Version: <b>v1.2.5</b>",
      );
  };
});

// Simple panel renderer for Logout/Help
function renderSimplePanel(title, content) {
  document.getElementById("main-panel").innerHTML =
    `<div class="panel-header">${title}</div><div class="tab-content">${content}</div>`;
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
window.onload = () => {
  fetch("/whoami")
    .then(res => res.json())
    .then(data => {
      applyRoleBasedMenu(data.role);
      loadConfig();

      if (data.role === "user") {
        applyViewOnlyMode();
      }
    })
    .catch(() => {
      applyRoleBasedMenu(null);
      loadConfig();
    });
};

