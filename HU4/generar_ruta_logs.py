async def generar_ruta_logs():
    import json
    import os
    from datetime import datetime
    import ast
    def safe_str(v):
        try:
            if v is None:
                return ""
            if isinstance(v, (bytes, bytearray)):
                try:
                    return bytes(v).decode("utf-8", errors="replace")
                except Exception:
                    return bytes(v).decode("cp1252", errors="replace")
            return str(v)
        except Exception:
            return ""

    def parse_config(raw):
        if isinstance(raw, dict):
            return raw
        t = safe_str(raw).strip()
        if not t:
            raise ValueError("vLocDicConfig vacio")
        try:
            return json.loads(t)
        except Exception:
            return ast.literal_eval(t)

    cfg = parse_config(GetVar("vLocDicConfig"))

    ruta_base = cfg['RutaLogs']

    fecha_actual = datetime.now()

    ano = "2025"
    mes = "11"
    dia = "30"

    parte_fecha = os.path.join(ano, mes, dia)

    ruta_sin_slash = os.path.join(ruta_base, parte_fecha)

    ruta_final = ruta_sin_slash + os.sep


    SetVar('vGblStrRutaLogs',ruta_final)
