def extract_dashboard_sources(cfg):
    sources = []

    # -------- Analog --------
    analog = cfg.get("ioSettings", {}).get("analog")
    if analog and analog["db"]["upload_local"]:
        sources.append({
            "name": "Analog Input",
            "db_name": analog["db"]["db_name"],
            "table": analog["db"]["table_name"] or "analog_data",
            "ts": "ts",
            "val": "value"
        })

    # -------- Digital Input --------
    di = cfg.get("ioSettings", {}).get("digitalInput")
    if di and di["db"]["upload_local"]:
        sources.append({
            "name": "Digital Input",
            "db_name": di["db"]["db_name"],
            "table": di["db"]["table_name"] or "digital_input",
            "ts": "ts",
            "val": "state"
        })

    # -------- PLC --------
    for plc in cfg.get("plc_configurations", []):
        db = plc["PLC"]["Database"]
        if db["upload_local"]:
            sources.append({
                "name": f'{plc["plcType"]} PLC',
                "db_name": db["db_name"],
                "table": db["table_name"],
                "ts": "ts",
                "val": "value"
            })

    # -------- Modbus RTU --------
    rtu = cfg.get("ModbusRTU", {})
    brands = rtu.get("Devices", {}).get("brands", {})
    for brand, bcfg in brands.items():
        for slave in bcfg.get("slaves", []):
            if slave["upload_local"]:
                sources.append({
                    "name": f"Modbus RTU {brand} S{slave['id']}",
                    "db_name": slave["db_name"],
                    "table": slave["table_name"],
                    "ts": "ts",
                    "val": "value"
                })

    return sources
