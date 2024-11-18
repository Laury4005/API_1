from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
import fdb
import config  #  DB Parms

app = FastAPI()

def get_db_connection():
    try:
        # Usamos los parámetros del archivo Config.py
        con = fdb.connect(dsn=config.DB_CONFIG['dsn'], 
                          user=config.DB_CONFIG['user'], 
                          password=config.DB_CONFIG['password'])
        print("Conexión exitosa")
        return con
    except Exception as e:
        print(f"Error de conexión: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")


@app.get("/consulta/{tabla}")
async def get_data(tabla: str, campo: str = Query(None), valor: str = Query(None)):
    query_mapping = {
        "fatipdoc": "SELECT ID_USUARIO, CLAVE, PREFIJO, TIPODEV FROM fatipdoc",
        "vendedor": "SELECT IDVEND, NOMBRE FROM vendedor",
        "actividad_eco_enc": "SELECT CODACT, DESCRIPCION, COD_INTERNACIONAL FROM actividad_eco_enc",
        "obligaciones_rut": "SELECT CODIGO, DESCRIPCION FROM obligaciones_rut",
        "tributos": "SELECT CODIGO, DESCRIPCION FROM tributos",
        "tributaria_tipocontribuyente": "SELECT CODIGO, DESCRIPCION FROM tributaria_tipocontribuyente",
        "tributaria_tipodocumento": "SELECT TDOC, DESCRIPCION FROM tributaria_tipodocumento",
        "paises": "SELECT ID_PAIS, PAIS FROM paises",
        "departamentos_elect": "SELECT ID_DEPTO, DEPARTAMENTO FROM departamentos_elect",
        "ciudades_elect": "SELECT ID_CIUDAD, CIUDAD FROM ciudades_elect",
        "cust": "SELECT ID_N, COMPANY, ADDR1, CITY, PAIS, PHONE1, GRAVABLE, CLIENTE, TIPOEMP, IDVEND, CV, FECHA_CREACION, EMAIL, DEPARTAMENTO, INACTIVO, REGIMEN, RESIDENTE FROM cust",
        "shipto": "SELECT ID_N, SUCCLIENTE, COMPANY, ADDR1, PHONE1, ID_VEND, PAIS, EMAIL, DEPARTAMENTO, PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, COD_DPTO, COD_MUNICIPIO, CITY, ESTADO, EMAIL_FAC_ELEC FROM shipto",
        # Add the rest of the tables here as in your original code...
    }

    if tabla not in query_mapping:
        raise HTTPException(status_code=404, detail="Tabla no encontrada")

    query = query_mapping[tabla]
    
    # Apply dynamic filters based on fields for each table
    filters = []
    if campo and valor:
        filters.append(f"{campo} = '{valor}'")
    
    # Combine filters if any
    if filters:
        query += " WHERE " + " AND ".join(filters)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        conn.close()

        # Convert the result into plain text
        result_text = "\n".join([str(row) for row in result])
        return PlainTextResponse(result_text, media_type="text/plain")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during query execution: {str(e)}")
    
# Bloque 1
@app.post("/insertar/cust")
async def insertar_cust(cliente: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'ID_N', 'COMPANY', 'ADDR1', 'CITY', 'PAIS', 'PHONE1', 'GRAVABLE', 'CLIENTE', 
        'TIPOEMP', 'IDVEND', 'CV', 'FECHA_CREACION', 'EMAIL', 'DEPARTAMENTO', 'REGIMEN', 'RESIDENTE'
    ]

    for field in required_fields:
        if field not in cliente:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")

    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla cust
        insert_query = """
        INSERT INTO cust (ID_N, COMPANY, ADDR1, CITY, PAIS, PHONE1, GRAVABLE, CLIENTE, 
                          TIPOEMP, IDVEND, CV, FECHA_CREACION, EMAIL, DEPARTAMENTO, INACTIVO, REGIMEN, RESIDENTE)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'N', ?, ?)
        """
        cursor.execute(insert_query, (
            cliente['ID_N'], cliente['COMPANY'], cliente['ADDR1'], cliente['CITY'], cliente['PAIS'], 
            cliente['PHONE1'], cliente['GRAVABLE'], cliente['CLIENTE'], cliente['TIPOEMP'], cliente['IDVEND'], 
            cliente['CV'], cliente['FECHA_CREACION'], cliente['EMAIL'], cliente['DEPARTAMENTO'], 
            cliente['REGIMEN'], cliente['RESIDENTE']
        ))
        conn.commit()

        # Retornar los valores insertados
        result_text = "\n".join([f"{key}: {value}" for key, value in cliente.items()])
        conn.close()
        return PlainTextResponse(result_text, media_type="text/plain")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla cust: {str(e)}")
    
@app.post("/insertar/shipto")
async def insertar_shipto(shipto: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'ID_N', 'SUCCLIENTE', 'COMPANY', 'ADDR1', 'PHONE1', 'ID_VEND', 'PAIS', 'EMAIL', 'DEPARTAMENTO',
        'PRIMER_APELLIDO', 'SEGUNDO_APELLIDO', 'PRIMER_NOMBRE', 'SEGUNDO_NOMBRE', 'FECHA_NACIMIENTO', 
        'COD_DPTO', 'COD_MUNICIPIO', 'CITY', 'ESTADO', 'EMAIL_FAC_ELEC'
    ]
    
    for field in required_fields:
        if field not in shipto:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")

    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla shipto
        insert_query = """
        INSERT INTO shipto (ID_N, SUCCLIENTE, COMPANY, ADDR1, PHONE1, ID_VEND, PAIS, EMAIL, DEPARTAMENTO, 
                           PRIMER_APELLIDO, SEGUNDO_APELLIDO, PRIMER_NOMBRE, SEGUNDO_NOMBRE, FECHA_NACIMIENTO, 
                           COD_DPTO, COD_MUNICIPIO, CITY, ESTADO, EMAIL_FAC_ELEC)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            shipto['ID_N'], shipto['SUCCLIENTE'], shipto['COMPANY'], shipto['ADDR1'], shipto['PHONE1'], 
            shipto['ID_VEND'], shipto['PAIS'], shipto['EMAIL'], shipto['DEPARTAMENTO'], shipto['PRIMER_APELLIDO'], 
            shipto['SEGUNDO_APELLIDO'], shipto['PRIMER_NOMBRE'], shipto['SEGUNDO_NOMBRE'], 
            shipto['FECHA_NACIMIENTO'], shipto['COD_DPTO'], shipto['COD_MUNICIPIO'], shipto['CITY'], 
            shipto['ESTADO'], shipto['EMAIL_FAC_ELEC']
        ))
        conn.commit()

        # Retornar los valores insertados
        result_text = "\n".join([f"{key}: {value}" for key, value in shipto.items()])
        conn.close()
        return PlainTextResponse(result_text, media_type="text/plain")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla shipto: {str(e)}")

@app.post("/insertar/tributaria")
async def insertar_tributaria(tributaria: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'ID_N', 'COMPANY', 'TDOC', 'CV', 'TIPO_CONTRIBUYENTE', 'PRIMER_NOMBRE', 'SEGUNDO_NOMBRE', 
        'PRIMER_APELLIDO', 'SEGUNDO_APELLIDO'
    ]
    
    for field in required_fields:
        if field not in tributaria:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")

    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla tributaria
        insert_query = """
        INSERT INTO tributaria (ID_N, COMPANY, TDOC, CV, TIPO_CONTRIBUYENTE, PRIMER_NOMBRE, SEGUNDO_NOMBRE, 
                               PRIMER_APELLIDO, SEGUNDO_APELLIDO)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            tributaria['ID_N'], tributaria['COMPANY'], tributaria['TDOC'], tributaria['CV'], 
            tributaria['TIPO_CONTRIBUYENTE'], tributaria['PRIMER_NOMBRE'], tributaria['SEGUNDO_NOMBRE'], 
            tributaria['PRIMER_APELLIDO'], tributaria['SEGUNDO_APELLIDO']
        ))
        conn.commit()

        # Retornar los valores insertados
        result_text = "\n".join([f"{key}: {value}" for key, value in tributaria.items()])
        conn.close()
        return PlainTextResponse(result_text, media_type="text/plain")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla tributaria: {str(e)}")

# Bloque 2
@app.post("/insertar/actividad_eco_det")
async def insertar_actividad_eco_det(actividad_eco: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = ['CODACT', 'ID_N', 'PRINCIPAL', 'COD_INTERNACIONAL']

    for field in required_fields:
        if field not in actividad_eco:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")

    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla actividad_eco_det
        insert_query = """
        INSERT INTO actividad_eco_det (CODACT, ID_N, PRINCIPAL, COD_INTERNACIONAL)
        VALUES (?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            actividad_eco['CODACT'], actividad_eco['ID_N'], actividad_eco['PRINCIPAL'], actividad_eco['COD_INTERNACIONAL']
        ))
        conn.commit()
        conn.close()

        return {"message": "Actividad económica insertada correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla actividad_eco_det: {str(e)}")

@app.post("/insertar/obligaciones_rutdet")
async def insertar_obligaciones_rutdet(obligacion: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = ['CODIGO', 'ID_N']

    for field in required_fields:
        if field not in obligacion:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")

    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla obligaciones_rutdet
        insert_query = """
        INSERT INTO obligaciones_rutdet (CODIGO, ID_N)
        VALUES (?, ?)
        """
        cursor.execute(insert_query, (
            obligacion['CODIGO'], obligacion['ID_N']
        ))
        conn.commit()
        conn.close()

        return {"message": "Obligación RUT insertada correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla obligaciones_rutdet: {str(e)}")

@app.post("/insertar/tributos_det")
async def insertar_tributos_det(tributo: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = ['CODIGO', 'ID_N']

    for field in required_fields:
        if field not in tributo:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")

    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla tributos_det
        insert_query = """
        INSERT INTO tributosdet (CODIGO, ID_N)
        VALUES (?, ?)
        """
        cursor.execute(insert_query, (
            tributo['CODIGO'], tributo['ID_N']
        ))
        conn.commit()
        conn.close()

        return {"message": "Tributo insertado correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla tributos_det: {str(e)}")

# Bloque 3
@app.post("/insertar/oe")
async def insertar_oe(datos_oe: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'ID_EMPRESA', 'ID_SUCURSAL', 'NUMBER', 'TIPO', 'ID_USUARIO', 'ID_N', 
        'SALESMAN', 'FECHA', 'DUEDATE', 'SUBTOTAL', 'COST', 'SALESTAX', 'DESTOTAL',
        'TOTAL', 'PAGOS', 'DEV_FACTURA', 'DEV_TIPOFACT', 'LETRAS', 'D', 'PORCENIVA',
        'FORPAGVAL', 'FORMAS_PAGO', 'HORACRE', 'CUFE', 'PREFIJO_POS'
    ]
    
    for field in required_fields:
        if field not in datos_oe:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")
    
    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla oe.
        insert_query = """
        INSERT INTO oe (
            ID_EMPRESA, ID_SUCURSAL, NUMBER, TIPO, ID_USUARIO, ID_N, SALESMAN, FECHA,
            DUEDATE, SUBTOTAL, COST, SALESTAX, DESTOTAL, TOTAL, PAGOS, DEV_FACTURA,
            DEV_TIPOFAC, LETRAS, D, PORCENIVA, FORPAGVAL, FORMAS_PAGO, HORCRE, CUFE, PREFIJO_POS
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            datos_oe['ID_EMPRESA'], datos_oe['ID_SUCURSAL'], datos_oe['NUMBER'], datos_oe['TIPO'],
            datos_oe['ID_USUARIO'], datos_oe['ID_N'], datos_oe['SALESMAN'], datos_oe['FECHA'],
            datos_oe['DUEDATE'], datos_oe['SUBTOTAL'], datos_oe['COST'], datos_oe['SALESTAX'],
            datos_oe['DESTOTAL'], datos_oe['TOTAL'], datos_oe['PAGOS'], datos_oe['DEV_FACTURA'],
            datos_oe['DEV_TIPOFAC'], datos_oe['LETRAS'], datos_oe['D'], datos_oe['PORCENIVA'],
            datos_oe['FORPAGVAL'], datos_oe['FORMAS_PAGO'], datos_oe['HORCRE'], datos_oe['CUFE'],
            datos_oe['PREFIJO_POS']
        ))
        conn.commit()

        # Retornar los valores insertados
        result_text = "\n".join([f"{key}: {value}" for key, value in datos_oe.items()])
        conn.close()

        return PlainTextResponse(result_text, media_type="text/plain")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla oe: {str(e)}")

@app.post("/insertar/oedet")
async def insertar_oedet(datos_oedet: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'CONTEO', 'ID_EMPRESA', 'ID_SUCURSAL', 'NUMBER', 'TIPO', 'ID_USUARIO', 'ITEM', 
        'LOCATION', 'IVA', 'QTYSHIP', 'PRICE', 'EXTEND', 'COST', 'TOTALDCT', 'VLR_IVA', 
        'PORC_IVA', 'PRECIOIVA', 'VLR_DCTOAD1', 'DPTO', 'CCOST', 'NUMITEM', 'COD_TALLA', 'CODBARRASCURVA'
    ]
    
    for field in required_fields:
        if field not in datos_oedet:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")
    
    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla oedet.
        insert_query = """
        INSERT INTO oedet (
            CONTEO, ID_EMPRESA, ID_SUCURSAL, NUMBER, TIPO, ID_USUARIO, ITEM, LOCATION, 
            IVA, QTYSHIP, PRICE, EXTEND, COST, TOTALDCT, VLR_IVA, PORC_IVA, PRECIOIVA, 
            VLR_DCTOAD1, DPTO, CCOST, NUMITEM, COD_TALLA, CODBARRASCURVA
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            datos_oedet['CONTEO'], datos_oedet['ID_EMPRESA'], datos_oedet['ID_SUCURSAL'], 
            datos_oedet['NUMBER'], datos_oedet['TIPO'], datos_oedet['ID_USUARIO'], 
            datos_oedet['ITEM'], datos_oedet['LOCATION'], datos_oedet['IVA'], 
            datos_oedet['QTYSHIP'], datos_oedet['PRICE'], datos_oedet['EXTEND'], 
            datos_oedet['COST'], datos_oedet['TOTALDCT'], datos_oedet['VLR_IVA'], 
            datos_oedet['PORC_IVA'], datos_oedet['PRECIOIVA'], datos_oedet['VLR_DCTOAD1'], 
            datos_oedet['DPTO'], datos_oedet['CCOST'], datos_oedet['NUMITEM'], 
            datos_oedet['COD_TALLA'], datos_oedet['CODBARRASCURVA']
        ))
        conn.commit()
        conn.close()

        return {"message": "Registro de oedet insertado correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla oedet: {str(e)}")

@app.post("/insertar/pagos")
async def insertar_pagos(datos_pagos: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'ID_EMPRESA', 'ID_SUCURSAL', 'NUMERO', 'TIPO', 'USUARIO', 'ACCT', 'CONCEPTO',
        'DESCRIPCION', 'PORC', 'FECHA', 'NUM_DOC', 'VLR_PAGO', 'CONTA', 'ID_N', 'VALORECIB', 'CONTEO'
    ]
    
    for field in required_fields:
        if field not in datos_pagos:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")
    
    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla pagos.
        insert_query = """
        INSERT INTO pagos (
            EMPRESA, SUCURSAL, NUMERO, TIPO, USUARIO, ACCT, CONCEPTO, DESCRIPCION, 
            PORC, FECHA, NUM_DOC, VLR_PAGO, CONTA, ID_N, VALRECIB, CONTEO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            datos_pagos['EMPRESA'], datos_pagos['SUCURSAL'], datos_pagos['NUMERO'], 
            datos_pagos['TIPO'], datos_pagos['USUARIO'], datos_pagos['ACCT'], datos_pagos['CONCEPTO'], 
            datos_pagos['DESCRIPCION'], datos_pagos['PORC'], datos_pagos['FECHA'], 
            datos_pagos['NUM_DOC'], datos_pagos['VLR_PAGO'], datos_pagos['CONTA'], 
            datos_pagos['ID_N'], datos_pagos['VALRECIB'], datos_pagos['CONTEO']
        ))
        conn.commit()
        conn.close()

        return {"message": "Registro de pagos insertado correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla pagos: {str(e)}")

@app.post("/insertar/itemact")
async def insertar_itemact(datos_itemact: dict):
    # Verificar que todos los campos requeridos estén presentes
    required_fields = [
        'LOCATION', 'ITEM', 'TIPO', 'BATCH', 'FECHA', 'QTY', 'NUMITEM', 
        'COD_TALLA', 'VALUNIT', 'COSTOP', 'TOTPARCIAL'
    ]
    
    for field in required_fields:
        if field not in datos_itemact:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")
    
    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar en la tabla itemact.
        insert_query = """
        INSERT INTO itemact (
            LOCATION, ITEM, TIPO, BATCH, FECHA, QTY, NUMITEM, COD_TALLA, 
            VALUNIT, COSTOP, TOTPARCIAL
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            datos_itemact['LOCATION'], datos_itemact['ITEM'], datos_itemact['TIPO'], 
            datos_itemact['BATCH'], datos_itemact['FECHA'], datos_itemact['QTY'], 
            datos_itemact['NUMITEM'], datos_itemact['COD_TALLA'], datos_itemact['VALUNIT'], 
            datos_itemact['COSTOP'], datos_itemact['TOTPARCIAL']
        ))
        conn.commit()
        conn.close()

        return {"message": "Registro de itemact insertado correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la tabla itemact: {str(e)}")


#Update
@app.put("/actualizar/shipto/{id_n}")
async def actualizar_shipto(id_n: int, datos_shipto: dict):
    # Asegurarse de que todos los campos sean proporcionados
    required_fields = ['ADDR1', 'PHONE1', 'EMAIL', 'EMAIL_FAC_ELEC']

    for field in required_fields:
        if field not in datos_shipto or datos_shipto[field] is None:
            raise HTTPException(status_code=400, detail=f"Falta el campo requerido: {field}")
    
    try:
        # Conexión a la base de datos Firebird
        conn = get_db_connection()
        cursor = conn.cursor()

        # Actualizar en la tabla shipto.
        update_query = """
        UPDATE SHIPTO
        SET ADDR1 = ?, PHONE1 = ?, EMAIL = ?, EMAIL_FAC_ELEC = ?
        WHERE ID_N = ?
        """
        cursor.execute(update_query, (
            datos_shipto['ADDR1'], datos_shipto['PHONE1'], datos_shipto['EMAIL'], datos_shipto['EMAIL_FAC_ELEC'], id_n
        ))
        conn.commit()
        conn.close()

        return {"message": "Datos de Shipto actualizados correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al actualizar en la tabla shipto: {str(e)}")