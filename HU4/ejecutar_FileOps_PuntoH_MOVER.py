# Author: Diego Ivan Lopez Ochoa
async def ejecutar_FileOps_PuntoH_MOVER():
    """
    Lee vLocJsonFileOpsPuntoH, mueve los archivos (MOVE) y genera ResultadosJson para FINALIZE.

    Guarda:
      - vLocJsonResultadosFileOpsPuntoH   (lista de dicts, 1 por ID_registro)
      - vLocStrResultadoSP
      - vLocStrResumenSP
    """
    import asyncio
    import json
    import os
    import shutil
    import traceback
    import unicodedata
    from collections import defaultdict

    def safe_str(v):
        try:
            if v is None:
                return ""
            return str(v)
        except Exception:
            return ""

    def to_ascii(s):
        try:
            s = "" if s is None else str(s)
            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii", "ignore")
            s = "".join(ch if 32 <= ord(ch) <= 126 else " " for ch in s)
            return " ".join(s.split())
        except Exception:
            return ""

    def set_error(user_msg, exc=None):
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", to_ascii(user_msg))
            SetVar("vLocJsonResultadosFileOpsPuntoH", "[]")
        except Exception:
            pass

    try:
        raw = safe_str(GetVar("vLocJsonFileOpsPuntoH")).strip()
        file_ops = json.loads(raw) if raw else []
    except Exception as e:
        set_error("ERROR Punto H FileOps | JSON invalido en vLocJsonFileOpsPuntoH", e)
        return False, None

    def run_move_sync():
        # agrupar por ID_registro para devolver 1 resultado por registro
        por_id = defaultdict(list)
        for op in file_ops:
            _id = safe_str(op.get("ID_registro")).strip()
            src = safe_str(op.get("RutaOrigenFull")).strip()
            dest_dir = safe_str(op.get("CarpetaDestino")).strip()
            filename = safe_str(op.get("NombreArchivo")).strip()

            if not filename and src:
                filename = os.path.basename(src)

            por_id[_id].append((src, dest_dir, filename))

        resultados = []
        ok_ids = 0
        fail_ids = 0

        for _id, items in por_id.items():
            ok = True
            err = ""
            nueva_ruta = ""  # SOLO si ok

            for (src, dest_dir, filename) in items:
                try:
                    if not _id:
                        raise ValueError("ID_registro vacio")
                    if not src:
                        raise ValueError("RutaOrigenFull vacia")
                    if not dest_dir:
                        raise ValueError("CarpetaDestino vacia")
                    if not filename:
                        raise ValueError("NombreArchivo vacio")

                    os.makedirs(dest_dir, exist_ok=True)
                    dst_full = os.path.join(dest_dir, filename)

                    # idempotencia: si destino ya existe y origen no, ok
                    if (not os.path.exists(src)) and os.path.exists(dst_full):
                        nueva_ruta = dest_dir
                        continue

                    # si ambos existen, intentamos overwrite seguro (re-ejecución)
                    if os.path.exists(src) and os.path.exists(dst_full):
                        try:
                            os.remove(dst_full)
                        except Exception as ex_rm:
                            raise RuntimeError(f"Destino ya existe y no se pudo borrar: {dst_full} | {safe_str(ex_rm)}")

                    if not os.path.exists(src):
                        raise FileNotFoundError(f"No existe origen: {src}")

                    shutil.move(src, dst_full)
                    nueva_ruta = dest_dir

                except Exception as e:
                    ok = False
                    err = safe_str(e)
                    break

            resultados.append({
                "ID_registro": _id,
                "MovimientoExitoso": bool(ok),
                "NuevaRutaArchivo": nueva_ruta if ok else "",
                "ErrorMsg": "" if ok else err
            })

            if ok:
                ok_ids += 1
            else:
                fail_ids += 1

        resumen = f"Punto H FileOps MOVE OK | IDs_OK={ok_ids} | IDs_FAIL={fail_ids} | TotalIDs={len(por_id)} | TotalArchivos={len(file_ops)}"
        return True, resultados, resumen

    try:
        loop = asyncio.get_running_loop()
        ok, resultados, resumen = await loop.run_in_executor(None, run_move_sync)

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", to_ascii(resumen))
            SetVar("vLocJsonResultadosFileOpsPuntoH", json.dumps(resultados, ensure_ascii=True))
        except Exception:
            pass

        return bool(ok), resultados

    except Exception as e:
        set_error("ERROR Punto H FileOps | fallo ejecucion MOVE", e)
        return False, None
