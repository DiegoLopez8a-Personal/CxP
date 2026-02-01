async def ejecutar_FileOps_PuntoI_COPIAR():
    import asyncio
    import json
    import os
    import shutil
    import traceback
    import unicodedata
    from collections import defaultdict

    def safe_str(v):
        try:
            return "" if v is None else str(v)
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
            SetVar("vLocJsonResultadosFileOpsPuntoI", "[]")
        except Exception:
            pass

    def build_src_full(ruta_origen, filename):
        ruta_origen = safe_str(ruta_origen).strip()
        filename = safe_str(filename).strip()
        if not ruta_origen or not filename:
            return ""
        r_low = ruta_origen.lower()
        f_low = filename.lower()
        if r_low.endswith(f_low):
            return ruta_origen
        if ruta_origen.endswith("\\") or ruta_origen.endswith("/"):
            return ruta_origen + filename
        return ruta_origen + "\\" + filename

    try:
        raw = safe_str(GetVar("vLocJsonFileOpsPuntoI")).strip()
        file_ops = json.loads(raw) if raw else []
    except Exception as e:
        set_error("ERROR Punto I FileOps | JSON invalido en vLocJsonFileOpsPuntoI", e)
        return False, None

    def run_copy_sync():
        por_id = defaultdict(list)

        for op in file_ops:
            _id = safe_str(op.get("ID_registro")).strip()
            ruta_origen = safe_str(op.get("RutaOrigen")).strip()
            nombres = safe_str(op.get("NombresArchivos")).strip()
            dest_dir = safe_str(op.get("CarpetaDestino")).strip()
            por_id[_id].append((ruta_origen, nombres, dest_dir))

        resultados = []
        ok_ids = 0
        fail_ids = 0
        total_archivos = 0

        for _id, packs in por_id.items():
            ok = True
            err = ""
            nueva_ruta = ""

            # Validar insumos: si no hay ruta o nombres => FAIL
            archivos = []
            for (ruta_origen, nombres, dest_dir) in packs:
                if not dest_dir:
                    ok = False
                    err = "CarpetaDestino vacia"
                    break
                if (not ruta_origen) or (not nombres):
                    ok = False
                    err = "No se logran identificar insumos (RutaOrigen o NombresArchivos vacio)"
                    break

                for nombre in [x.strip() for x in nombres.split(";") if x.strip()]:
                    archivos.append((build_src_full(ruta_origen, nombre), dest_dir, nombre))

            if ok:
                try:
                    for (src_full, dest_dir, nombre) in archivos:
                        total_archivos += 1
                        os.makedirs(dest_dir, exist_ok=True)
                        dst_full = os.path.join(dest_dir, nombre)

                        if (not os.path.exists(src_full)) and os.path.exists(dst_full):
                            nueva_ruta = dest_dir
                            continue

                        if not os.path.exists(src_full):
                            raise FileNotFoundError(f"No existe origen: {src_full}")

                        if os.path.exists(dst_full):
                            try:
                                os.remove(dst_full)
                            except Exception as ex_rm:
                                raise RuntimeError(f"Destino existe y no se pudo borrar: {dst_full} | {safe_str(ex_rm)}")

                        shutil.copy2(src_full, dst_full)
                        nueva_ruta = dest_dir

                except Exception as e:
                    ok = False
                    err = safe_str(e)

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

        resumen = f"Punto I FileOps COPY OK | IDs_OK={ok_ids} | IDs_FAIL={fail_ids} | TotalIDs={len(por_id)} | TotalArchivos={total_archivos}"
        return True, resultados, resumen

    try:
        loop = asyncio.get_running_loop()
        ok, resultados, resumen = await loop.run_in_executor(None, run_copy_sync)

        SetVar("vLocStrResultadoSP", bool(ok))
        SetVar("vLocStrResumenSP", to_ascii(resumen))
        SetVar("vLocJsonResultadosFileOpsPuntoI", json.dumps(resultados, ensure_ascii=True))
        return bool(ok), resultados

    except Exception as e:
        set_error("ERROR Punto I FileOps | fallo ejecucion COPY", e)
        return False, None
