/*ENTREGA 5
*FECHA DE ENTREGA: 20/06/2025
*COMISION:5600
*NUMERO DE GRUPO: 08
*NOMBRE DE LA MATERIA: Base de Datos Aplicadas
*INTEGRANTES: 45318374 | Di Marco Jazmín
			  46346548 | Medina Federico Gabriel
			  42905305 | Mendez Samuel Omar
			  44588998 | Valdevieso Rocío Elizabeth
*/
USE Com5600G08
go
----------importo los datos que están en archivo de los pagos
CREATE OR ALTER PROCEDURE ddbbaTP.Importar_Pagos 
    @rutaArchivo VARCHAR(200)
AS
BEGIN
    -- Borra si ya existe la tabla temporal global
    IF OBJECT_ID('tempdb..##Carga_Pago_Temp') IS NOT NULL 
        DROP TABLE ##Carga_Pago_Temp;

    -- Crea la tabla temporal con colId como BIGINT
    CREATE TABLE ##Carga_Pago_Temp
    (
        colId BIGINT,
        colFecha VARCHAR(30),
        colResp VARCHAR(10),
        colValor DECIMAL(10,2),
        colMedioPago VARCHAR(30)
    );

    -- Arma el comando BULK INSERT
    DECLARE @SQL_Pago NVARCHAR(MAX) = '
        BULK INSERT ##Carga_Pago_Temp
        FROM ''' + @rutaArchivo + '''
        WITH (
            FIELDTERMINATOR = '';'',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            CODEPAGE = ''ACP''
        );';

    EXEC sp_executesql @SQL_Pago;

    --SELECT * FROM ##Carga_Pago_Temp;
END;
go

CREATE OR ALTER PROCEDURE ddbbaTP.ActualizarTablasSegunPagosArchivo
AS
BEGIN
    DECLARE 
        @IdCuota INT,
        @IdFactura INT,
        @IdCuenta INT,
        @IdMedioPago INT,
        @colId BIGINT,
        @colFecha VARCHAR(30),
        @colResp VARCHAR(10),
        @colValor DECIMAL(10,2),
        @colMedioPago VARCHAR(30);

    DECLARE pagos_cursor CURSOR FOR
        SELECT colId, colFecha, colResp, colValor, colMedioPago
        FROM ##Carga_Pago_Temp;

    OPEN pagos_cursor;

    FETCH NEXT FROM pagos_cursor 
    INTO @colId, @colFecha, @colResp, @colValor, @colMedioPago;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            -- 1. Insertar cuota
            INSERT INTO ddbbaTP.Cuota (Estado, NroSocio)
            VALUES ('Pendiente', @colResp);

            SET @IdCuota = SCOPE_IDENTITY();

            -- 2. Insertar factura
            INSERT INTO ddbbaTP.Factura (Fecha_Vencimiento, Dias_Atrasados, Estado, IdDescuento, IdCuota)
            VALUES (@colFecha, 0, 'Pagada', NULL, @IdCuota);

            SET @IdFactura = SCOPE_IDENTITY();

            -- 3. Buscar o crear medio de pago
            SELECT @IdMedioPago = IdMedioPago
            FROM ddbbaTP.MedioDePago
            WHERE Nombre = @colMedioPago;

            IF @IdMedioPago IS NULL
            BEGIN
                INSERT INTO ddbbaTP.MedioDePago (Nombre, Tipo, Modalidad)
                VALUES (@colMedioPago, 'Desconocido', 'Importado');

                SET @IdMedioPago = SCOPE_IDENTITY();
            END

            -- 4. Buscar o crear cuenta
            SELECT @IdCuenta = IdCuenta
            FROM ddbbaTP.Cuenta
            WHERE NroSocio = @colResp;

            IF @IdCuenta IS NULL
            BEGIN
                INSERT INTO ddbbaTP.Cuenta (Saldo_Favor, NroSocio)
                VALUES (0, @colResp);

                SET @IdCuenta = SCOPE_IDENTITY();
            END

            -- 5. Insertar pago usando colId como IdPago
            INSERT INTO ddbbaTP.Pago (IdPago, Fecha_de_Pago, IdCuenta, IdFactura, IdMedioDePago, Monto)
            VALUES (@colId, @colFecha, @IdCuenta, @IdFactura, @IdMedioPago, @colValor);

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            ROLLBACK TRANSACTION;
            PRINT 'Error en colId: ' + CAST(@colId AS VARCHAR) + ' - ' + ERROR_MESSAGE();
        END CATCH;

        FETCH NEXT FROM pagos_cursor 
        INTO @colId, @colFecha, @colResp, @colValor, @colMedioPago;
    END;

    CLOSE pagos_cursor;
    DEALLOCATE pagos_cursor;
END;
go

execute ddbbaTP.Importar_Pagos 'C:\_temp\Pago cuotas.csv'
go

execute ddbbaTP.ActualizarTablasSegunPagosArchivo
go

----------NUEVAS CUOTAS Y FACTURAS

CREATE OR ALTER PROCEDURE ddbbaTP.GenerarCuotaYFacturaMembresiaYActividades
    @NroSocio VARCHAR(10),
    @Tipo VARCHAR(20), -- 'Categoria' | 'Actividad'
    @NombreActividad VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @IdCuota INT, @IdDescuento INT = NULL, @PorcentajeDescuento INT = 0, @MontoFinal DECIMAL(10,2);
        DECLARE @IdFactura INT, @FechaVencimiento DATE, @MontoBase DECIMAL(10,2);
        DECLARE @FechaNacimientoSocio VARCHAR(10), @FechaNacimientoSocioDATE DATE, @EdadSocio INT;
        DECLARE @IdGrupoFamiliarSocio INT, @NroSocioResponsable VARCHAR(10), @CantAct INT;
        DECLARE @EsSocio BIT = 1; -- En este procedimiento, siempre es un socio.

        -- Obtener el grupo familiar del socio
        SELECT @IdGrupoFamiliarSocio = s.IdGrupoFamiliar
        FROM ddbbaTP.Socio s
        WHERE s.NroSocio = @NroSocio;

        -- Determinar el socio responsable
        IF @IdGrupoFamiliarSocio IS NOT NULL
        BEGIN
            SELECT @NroSocioResponsable = gf.NroSocio
            FROM ddbbaTP.GrupoFamiliar gf
            WHERE gf.IdGrupoFamiliar = @IdGrupoFamiliarSocio;
        END
        ELSE
        BEGIN
            SET @NroSocioResponsable = @NroSocio;
        END

        -- Insertar cuota
        INSERT INTO ddbbaTP.Cuota (Estado, NroSocio, Socio_Cuota)
        VALUES ('Pendiente', @NroSocioResponsable, @NroSocio);
        SET @IdCuota = SCOPE_IDENTITY();

        -- Obtener y convertir fecha de nacimiento
        SELECT @FechaNacimientoSocio = Fecha_De_Nacimiento
        FROM ddbbaTP.Socio
        WHERE NroSocio = @NroSocio;

        SET @FechaNacimientoSocioDATE = TRY_CONVERT(DATE, @FechaNacimientoSocio, 103);
        IF @FechaNacimientoSocioDATE IS NULL
        BEGIN
            THROW 50000, 'La fecha de nacimiento del socio no es válida o está en un formato incorrecto.', 1;
        END

        -- Calcular edad
        SET @EdadSocio = DATEDIFF(YEAR, @FechaNacimientoSocioDATE, GETDATE()) -
            CASE WHEN MONTH(@FechaNacimientoSocioDATE) > MONTH(GETDATE()) OR
                      (MONTH(@FechaNacimientoSocioDATE) = MONTH(GETDATE()) AND DAY(@FechaNacimientoSocioDATE) > DAY(GETDATE()))
                      THEN 1
                      ELSE 0
            END;

        -- Determinar el monto según tipo
        IF @Tipo = 'Categoria'
        BEGIN
            EXEC ddbbaTP.Asignar_Monto_Categoria @EdadS = @EdadSocio, @MontoBaseS = @MontoBase OUTPUT;

            IF @IdGrupoFamiliarSocio IS NOT NULL
            BEGIN
                SET @PorcentajeDescuento = 15;
            END
        END
        ELSE IF @Tipo = 'Actividad'
        BEGIN
            -- Si @NombreActividad es NULL o vacío, intentar obtener la última actividad inscrita
            IF @NombreActividad IS NULL OR @NombreActividad = ''
            BEGIN
                SELECT TOP 1 @NombreActividad = a.Nombre
                FROM ddbbaTP.Inscripto i
                JOIN ddbbaTP.Actividad a ON i.IdActividad = a.IdActividad
                WHERE i.NroSocio = @NroSocio
                  AND TRY_CONVERT(DATE, i.FechaInscripcion, 103) IS NOT NULL
                ORDER BY TRY_CONVERT(DATE, i.FechaInscripcion, 103) DESC, i.IdActividad DESC;
            END

            IF @NombreActividad IS NULL OR @NombreActividad = ''
            BEGIN
                THROW 50001, 'El socio no tiene inscripciones válidas con fecha correcta (d/m/yyyy) o no se especificó una actividad.', 1;
            END

            -- Para actividades, usar el procedimiento Asignar_Monto_Actividad
            EXEC ddbbaTP.Asignar_Monto_Actividad @NombreAct = @NombreActividad, @MontoBaseS = @MontoBase OUTPUT;

            -- Verificar si tiene más de una actividad para descuento
            SELECT @CantAct = COUNT(*) FROM ddbbaTP.Inscripto
            WHERE NroSocio = @NroSocio;
            IF @CantAct > 1
            BEGIN
                SET @PorcentajeDescuento = 10;
            END
        END
        ELSE
        BEGIN
            THROW 50000, 'Tipo de cuota no válido. Debe ser "Categoria" o "Actividad".', 1;
        END

        -- Calcular monto final
        SET @MontoFinal = ISNULL(@MontoBase, 0) - (ISNULL(@MontoBase, 0) * @PorcentajeDescuento / 100.0);

        -- Fecha de vencimiento
        SET @FechaVencimiento = CAST(DATEADD(DAY, 5, GETDATE()) AS DATE); --le asigno una fecha al primer vencimiento

        -- Buscar o generar el descuento correspondiente
        IF @PorcentajeDescuento > 0
        BEGIN
            SELECT @IdDescuento = d.IdDescuento
            FROM ddbbaTP.Descuento d
            WHERE d.Porcentaje = @PorcentajeDescuento AND d.NroSocio = @NroSocio;

            IF @IdDescuento IS NULL
            BEGIN
                INSERT INTO ddbbaTP.Descuento (Porcentaje, NroSocio)
                VALUES (@PorcentajeDescuento, @NroSocio);

                SET @IdDescuento = SCOPE_IDENTITY();
            END
        END

        -- Insertar factura
        INSERT INTO ddbbaTP.Factura (Fecha_Vencimiento, Monto_Total, Dias_Atrasados, Estado, IdDescuento, IdCuota, Detalle)
        VALUES (
            @FechaVencimiento,
            @MontoFinal,
            0,
            'Pendiente',
            @IdDescuento,
            @IdCuota,
            @NombreActividad
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

---------->comprobación de que se inserta y modifica la factura
select * from ddbbaTP.Factura
go
execute ddbbaTP.GenerarCuotaYFacturaMembresiaYActividades @NroSocio ='SN-4139', @Tipo ='Actividad' , @NombreActividad='Futsal'
go
select * from ddbbaTP.Factura
go
--------------------------------------------------------------------------------------------------------------------------------
---------HACER GENERAR FACTURA ACTIVIDAD EXTRA
CREATE OR ALTER PROCEDURE ddbbaTP.GenerarCuotaYFacturaActividadExtra
    @NroPersona VARCHAR(10),            -- Puede ser NroSocio o IdInvitado (como string)
    @NombreActividad VARCHAR(50)        -- 'Sum Recreativo', 'Colonia', o 'Pileta'
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @IdCuota INT, @IdFactura INT, @MontoBase DECIMAL(10,2);
        DECLARE @FechaNacimiento DATE, @Edad INT;
        DECLARE @IdGrupoFamiliar INT = NULL;
        DECLARE @Responsable VARCHAR(10); -- Será el NroSocio del socio principal o patrocinador
        DECLARE @FechaVencimiento DATE = DATEADD(DAY, 30, GETDATE());
        DECLARE @EsSocio BIT, @Existe BIT;
        DECLARE @IdInvitadoInt INT; -- Para la conversión de @NroPersona a INT si es invitado

        PRINT 'DEBUG: Iniciar GenerarCuotaYFacturaActividadExtra para @NroPersona = ' + @NroPersona + ', @NombreActividad = ' + @NombreActividad;

        -- 1. Detectar tipo de persona
        EXEC ddbbaTP.Validar_TipoPersona
            @Identificador = @NroPersona,
            @EsSocio = @EsSocio OUTPUT,
            @Existe = @Existe OUTPUT;

        IF @Existe = 0
            THROW 50000, 'El identificador no corresponde a un socio ni a un invitado.', 1;

        PRINT 'DEBUG: @EsSocio = ' + CAST(@EsSocio AS VARCHAR(1)) + ', @Existe = ' + CAST(@Existe AS VARCHAR(1));

        -- 2. Obtener fecha de nacimiento y responsable
        IF @EsSocio = 1 -- La persona es un Socio
        BEGIN
            SELECT @FechaNacimiento = TRY_CONVERT(DATE, Fecha_De_Nacimiento, 103),
                   @IdGrupoFamiliar = IdGrupoFamiliar
            FROM ddbbaTP.Socio
            WHERE NroSocio = @NroPersona;

            -- Si el socio pertenece a un grupo familiar, el responsable es el NroSocio principal del grupo.
            -- De lo contrario, el socio mismo es el responsable.
            SET @Responsable = ISNULL(
                (SELECT NroSocio FROM ddbbaTP.GrupoFamiliar WHERE IdGrupoFamiliar = @IdGrupoFamiliar),
                @NroPersona
            );
            PRINT 'DEBUG: Es Socio. NroSocio responsable = ' + ISNULL(@Responsable, 'NULL') + ', FechaNacimiento = ' + ISNULL(CONVERT(VARCHAR(10), @FechaNacimiento, 120), 'NULL');
        END
        ELSE -- La persona es un Invitado
        BEGIN
            SET @IdInvitadoInt = TRY_CAST(@NroPersona AS INT); -- Convertir el IdInvitado a INT

            IF @IdInvitadoInt IS NULL
                THROW 50000, 'El identificador de invitado proporcionado no es un número válido.', 1;

            -- Obtener fecha de nacimiento y, CRUCIALMENTE, el Nro_Socio del patrocinador del invitado
            SELECT @FechaNacimiento = TRY_CONVERT(DATE, Fecha_De_Nacimiento, 103),
                   @Responsable = Nro_Socio -- Aquí obtenemos el Nro_Socio del socio que patrocina al invitado
            FROM ddbbaTP.Invitado
            WHERE IdInvitado = @IdInvitadoInt;

            -- Validar que el invitado tenga un socio responsable
            IF @Responsable IS NULL
                THROW 50000, 'El invitado no tiene un socio responsable asignado (Nro_Socio es NULL en Invitado).', 1;

            PRINT 'DEBUG: Es Invitado. IdInvitado = ' + CAST(@IdInvitadoInt AS VARCHAR(10)) + ', NroSocio responsable = ' + ISNULL(@Responsable, 'NULL') + ', FechaNacimiento = ' + ISNULL(CONVERT(VARCHAR(10), @FechaNacimiento, 120), 'NULL');
        END

        IF @FechaNacimiento IS NULL
            THROW 50010, 'Fecha de nacimiento inválida o no encontrada para el identificador proporcionado.', 1;

        SET @Edad = DATEDIFF(YEAR, @FechaNacimiento, GETDATE()) -
                     CASE WHEN MONTH(@FechaNacimiento) > MONTH(GETDATE()) OR
                                 (MONTH(@FechaNacimiento) = MONTH(GETDATE()) AND DAY(@FechaNacimiento) > DAY(GETDATE()))
                                 THEN 1 ELSE 0 END;
        PRINT 'DEBUG: Edad calculada = ' + CAST(@Edad AS VARCHAR(3));


        -- 3. Determinar monto base
        IF @NombreActividad = 'Pileta'
        BEGIN
            EXEC ddbbaTP.Asignar_Monto_Pileta
                @EdadPersona = @Edad,
                @EsSocio = @EsSocio,
                @TipoPase = 'Día', -- Asumimos 'Día' por defecto, ajustar si es necesario
                @MontoBaseS = @MontoBase OUTPUT;
            PRINT 'DEBUG: Actividad Pileta. MontoBase = ' + ISNULL(CAST(@MontoBase AS VARCHAR(15)), 'NULL');
        END
        ELSE IF @NombreActividad = 'Sum Recreativo'
        BEGIN
            SELECT TOP 1 @MontoBase = Precio
            FROM ddbbaTP.Sum_Recreativo;
            PRINT 'DEBUG: Actividad Sum Recreativo. MontoBase = ' + ISNULL(CAST(@MontoBase AS VARCHAR(15)), 'NULL');
        END
        ELSE IF @NombreActividad = 'Colonia'
        BEGIN
            SELECT TOP 1 @MontoBase = Precio
            FROM ddbbaTP.Colonia;
            PRINT 'DEBUG: Actividad Colonia. MontoBase = ' + ISNULL(CAST(@MontoBase AS VARCHAR(15)), 'NULL');
        END
        ELSE
        BEGIN
            THROW 50020, 'Nombre de actividad no válido. Debe ser "Pileta", "Sum Recreativo" o "Colonia".', 1;
        END

        IF @MontoBase IS NULL
            THROW 50030, 'No se pudo determinar el monto base para la actividad especificada.', 1;

        -- 4. Insertar cuota
        INSERT INTO ddbbaTP.Cuota (Estado, NroSocio, Socio_Cuota)
        VALUES (
            'Pendiente',
            @Responsable, -- El NroSocio de la cuota será el responsable (socio principal o patrocinador)
            CASE WHEN @EsSocio = 1 THEN @NroPersona ELSE NULL END -- Socio_Cuota es el NroSocio si es socio, NULL si es invitado
        );
        SET @IdCuota = SCOPE_IDENTITY();
        PRINT 'DEBUG: Cuota insertada. IdCuota = ' + CAST(@IdCuota AS VARCHAR(10));

        -- 5. Insertar factura (sin descuentos)
        INSERT INTO ddbbaTP.Factura (
            Fecha_Vencimiento,
            Monto_Total,
            Dias_Atrasados,
            Estado,
            IdDescuento,
            IdCuota,
            Detalle
        )
        VALUES (
            @FechaVencimiento,
            @MontoBase,
            0,
            'Pendiente',
            NULL, -- Sin descuento inicial
            @IdCuota,
            @NombreActividad
        );
        SET @IdFactura = SCOPE_IDENTITY(); -- Obtener el ID de la factura recién insertada
        PRINT 'DEBUG: Factura insertada. IdFactura = ' + CAST(@IdFactura AS VARCHAR(10));

        -- 6. Actualizar el campo IdFactura del invitado si la persona es un invitado
        IF @EsSocio = 0
        BEGIN
            UPDATE ddbbaTP.Invitado
            SET IdFactura = @IdFactura
            WHERE IdInvitado = @IdInvitadoInt; -- Usar la variable INT para el IdInvitado
            PRINT 'DEBUG: Invitado ' + CAST(@IdInvitadoInt AS VARCHAR(10)) + ' actualizado con IdFactura = ' + CAST(@IdFactura AS VARCHAR(10));
        END

        COMMIT TRANSACTION;
        PRINT 'DEBUG: Transacción completada exitosamente. Cuota y Factura generadas.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @Msg NVARCHAR(4000) = ERROR_MESSAGE();
        PRINT 'ERROR en GenerarCuotaYFacturaActividadExtra: ' + @Msg;
        THROW; -- Relanzar el error original
    END CATCH
END;
GO
select * from ddbbaTP.Factura
go
execute ddbbaTP.GenerarCuotaYFacturaActividadExtra     @NroPersona = '5' ,  @NombreActividad= 'Pileta' 
GO
select * from ddbbaTP.Factura
go

---------- ------------------------------------ ANULAR última FACTURA  de un socio que se dio de baja
CREATE OR ALTER PROCEDURE ddbbaTP.AnularFacturasPorBajaSocio @NroSocio VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON; 
    BEGIN TRY
        DECLARE @IdFacturaUltima INT;

        -- Encuentra el IdFactura de la última factura generada para el socio dado
        SELECT TOP 1 @IdFacturaUltima = f.IdFactura
        FROM ddbbaTP.Factura AS f
        INNER JOIN ddbbaTP.Cuota AS c ON f.IdCuota = c.IdCuota
        WHERE c.NroSocio = @NroSocio
        ORDER BY f.IdFactura DESC; -- Asume que un IdFactura más alto significa que fue generada más recientemente

        -- Verifica si se encontró una factura antes de intentar anularla
        IF @IdFacturaUltima IS NOT NULL
        BEGIN
            -- Actualiza el estado de la última factura encontrada a 'Anulada'
            UPDATE f
            SET f.Estado = 'Anulada'
            FROM ddbbaTP.Factura AS f
            WHERE f.IdFactura = @IdFacturaUltima;

            -- Mensaje de confirmación (solo para fines de desarrollo/depuración)
            SELECT 'Última factura (' + CAST(@IdFacturaUltima AS VARCHAR(10)) + ') anulada exitosamente para el socio: ' + @NroSocio AS Mensaje;
        END
        ELSE
        BEGIN
            -- Mensaje si no se encontraron facturas para el socio
             SELECT 'No se encontraron facturas para el socio: ' + @NroSocio AS Mensaje;
        END

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX), @ErrorSeverity INT, @ErrorState INT;
        SELECT
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
go

CREATE OR ALTER PROCEDURE Dar_Baja_Socio @NroSocio VARCHAR(10)
AS
BEGIN
	UPDATE ddbbaTP.Socio SET Estado= 'Inactivo' where NroSocio= @NroSocio
	PRINT 'Socio ' + @NroSocio + ' dado de baja'
END;
go
EXECUTE Dar_Baja_Socio @NroSocio= 'SN-4003'
go
EXECUTE ddbbaTP.AnularFacturasPorBajaSocio @NroSocio='SN-4003'
go

--NOTA: No se actualizan las clases a las que está anotado el socio ni su grupo familiar por el momento

------------------ HabilitarPasePileta  ------------------

CREATE OR ALTER PROCEDURE ddbbaTP.HabilitarPasePileta
    @TarifaSocio DECIMAL(10,2),
    @TarifaInvitado DECIMAL(10,2),
    @NroSocio VARCHAR(10),
    @IdInvitado INT = NULL, 
    @Fec_Temporada DATE 
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @IdActividadExtra INT; 
    DECLARE @IsValidEntity BIT = 0; 
    DECLARE @PasePiletaExistente INT;

    IF @TarifaSocio < 0 OR @TarifaInvitado < 0
    BEGIN
        PRINT 'Error: Tarifa no puede ser negativa.';
        RETURN -1; 
    END;

    IF @IdInvitado IS NOT NULL 
    BEGIN
        IF EXISTS (SELECT 1 FROM ddbbaTP.Invitado WHERE Nro_Socio = @NroSocio AND IdInvitado = @IdInvitado)
        BEGIN
            SET @IsValidEntity = 1;
        END
        ELSE
        BEGIN
            PRINT 'Error: No hay match entre socio e invitado.';
            RETURN -2; 
        END;
    END
    ELSE 
    BEGIN
        
        IF EXISTS (SELECT 1 FROM ddbbaTP.Socio WHERE NroSocio = @NroSocio)
        BEGIN
            SET @IsValidEntity = 1;
        END
        ELSE
        BEGIN
            PRINT 'Error: El socio no existe ne la base de datos.';
            RETURN -4; 
        END;
    END;

    IF @IsValidEntity = 1
    BEGIN
        SELECT @IdActividadExtra = IdActividadExtra
        FROM ddbbaTP.Pileta
        WHERE Fec_Temporada = @Fec_Temporada;

        IF @IdActividadExtra IS NULL
        BEGIN
            PRINT 'Error: La pileta no está disponible en esa fecha.';
            RETURN -3; 
        END;

        BEGIN TRY

            SELECT @PasePiletaExistente = IdPasePileta
            FROM ddbbaTP.PasePileta
            WHERE NroSocio = @NroSocio
              AND (IdInvitado = @IdInvitado OR (IdInvitado IS NULL AND @IdInvitado IS NULL))
              AND IdPileta = @IdActividadExtra;

            IF @PasePiletaExistente IS NOT NULL
            BEGIN
                UPDATE ddbbaTP.PasePileta
                SET EstadoPase = 'Activo',
                    Tarifa_Socio = @TarifaSocio,
                    Tarifa_Invitado = @TarifaInvitado 

                WHERE IdPasePileta = @PasePiletaExistente;
                PRINT 'Se Activó el pase pileta.';
            END
            ELSE
            BEGIN
               
                INSERT INTO ddbbaTP.PasePileta (
                    Tarifa_Socio,
                    Tarifa_Invitado,
                    NroSocio,
                    IdInvitado,
                    IdPileta, 
                    EstadoPase
                )
                VALUES (
                    @TarifaSocio, @TarifaInvitado, @NroSocio, @IdInvitado, @IdActividadExtra,   'Activo'
                );
                PRINT 'Se insertó el pase correctamente.';
                SELECT IdPasePileta = SCOPE_IDENTITY();  
            END;
        END TRY
        BEGIN CATCH
            PRINT 'Error insewrtando pase pileta: ' + ERROR_MESSAGE();
            RETURN -99; 
        END CATCH;
    END;
END;
go

-------------------------------------------------- PAGAR CUOTA/factura
CREATE OR ALTER PROCEDURE ddbbaTP.PagarFactura
    @IdFactura INT,
    @MedioPago INT,
    @Valor DECIMAL(10,2)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
            DECLARE @MontoFactura DECIMAL(10,2), @EstadoFactura VARCHAR(20), @NroSocio VARCHAR(10);
            DECLARE @FechaVenc DATE, @DiasAtrasados INT, @IdCuota INT;
            DECLARE @FechaPago DATE;
            DECLARE @DetalleFactura VARCHAR(50); -- Variable para el detalle de la factura

            SET @FechaPago= GETDATE();

            SELECT
                @MontoFactura = Monto_Total,
                @EstadoFactura = Estado,
                @FechaVenc = TRY_CAST(Fecha_Vencimiento AS DATE),
                @IdCuota = IdCuota,
                @DetalleFactura = Detalle 
            FROM ddbbaTP.Factura
            WHERE IdFactura = @IdFactura;

            IF @MontoFactura IS NULL
                THROW 60001, 'Invoice not found.', 1;

            IF @FechaVenc IS NULL
                THROW 60006, 'Invoice due date is not valid.', 1;

            -- 2. 
            IF @Valor <> @MontoFactura
                THROW 60003, 'The amount paid does not match the invoice total.', 1;

            -- 3. 
            SELECT @NroSocio = I.Nro_Socio
            FROM ddbbaTP.Invitado I
            WHERE I.IdFactura = @IdFactura;

            IF @NroSocio IS NULL
            BEGIN
                SELECT @NroSocio = C.NroSocio
                FROM ddbbaTP.Cuota C
                WHERE C.IdCuota = @IdCuota;
            END;

            IF @NroSocio IS NULL
                THROW 60004, 'Could not determine the responsible member for this invoice. The invoice is not linked to a guest or directly to a member.', 1;

            -- 4. existe socio¿?
            IF NOT EXISTS (SELECT 1 FROM ddbbaTP.Cuenta WHERE NroSocio = @NroSocio)
                THROW 60005, 'No account exists for the responsible member with number: ', 1;

            -- 5. Insert pago
            DECLARE @NuevoIdPago BIGINT = (SELECT ISNULL(MAX(IdPago), 0) + 1 FROM ddbbaTP.Pago);

            INSERT INTO ddbbaTP.Pago (IdPago, Fecha_de_Pago, Monto, IdFactura, IdMedioDePago)
            VALUES (@NuevoIdPago, GETDATE(), @Valor, @IdFactura, @MedioPago);

            -- 6. Update cuenta
            UPDATE ddbbaTP.Cuenta
            SET Saldo_Favor = ISNULL(Saldo_Favor, 0) + @Valor
            WHERE NroSocio = @NroSocio;

            -- 7. Update  
            SET @DiasAtrasados = DATEDIFF(DAY, @FechaVenc, GETDATE());
            IF @DiasAtrasados < 0 SET @DiasAtrasados = 0;

            UPDATE ddbbaTP.Factura
            SET Estado = 'Pagada',
                Dias_Atrasados = @DiasAtrasados
            WHERE IdFactura = @IdFactura;

            ------------------ habilitar pase------------------
            DECLARE @CurrentIdInvitado INT;
            DECLARE @CurrentNroSocioAsociadoInvitado VARCHAR(10);
            DECLARE @PasePiletaTarifaSocio DECIMAL(10,2);
            DECLARE @PasePiletaTarifaInvitado DECIMAL(10,2);
            DECLARE @EsFacturaDePileta BIT = 0;
            DECLARE @FecTemporadaCalculada DATE; 
			 
            SET @PasePiletaTarifaSocio = 0.00;
            SET @PasePiletaTarifaInvitado = 0.00;
			 
            SELECT @CurrentIdInvitado = I.IdInvitado,
                   @CurrentNroSocioAsociadoInvitado = I.Nro_Socio
            FROM ddbbaTP.Invitado I
            WHERE I.IdFactura = @IdFactura;
			 
            SET @FecTemporadaCalculada = DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0);

            IF TRIM(@DetalleFactura) = 'Pileta'
            BEGIN
                SET @EsFacturaDePileta = 1;
				 
                IF @CurrentIdInvitado IS NOT NULL AND @CurrentNroSocioAsociadoInvitado IS NOT NULL
                BEGIN
                    SET @PasePiletaTarifaInvitado = @MontoFactura;
                END 
                ELSE IF EXISTS (SELECT 1 FROM ddbbaTP.Cuota C WHERE C.IdCuota = @IdCuota AND C.NroSocio = @NroSocio)
                BEGIN
                    SET @PasePiletaTarifaSocio = @MontoFactura; 
                    SET @CurrentIdInvitado = NULL;
                    SET @CurrentNroSocioAsociadoInvitado = @NroSocio; -- The member is the "responsible" and "associated person"
                END
                  ELSE
                BEGIN 
                    PRINT 'WARNING: Pool invoice found but could not link to a specific guest or direct member.';
                END;
            END;

            -- Debug messages
            PRINT 'DEBUG: DetalleFactura = ''' + ISNULL(@DetalleFactura, 'NULL') + '''';
            PRINT 'DEBUG: EsFacturaDePileta = ' + CAST(@EsFacturaDePileta AS VARCHAR(1));
            PRINT 'DEBUG: CurrentIdInvitado = ' + ISNULL(CAST(@CurrentIdInvitado AS VARCHAR(10)), 'NULL');
            PRINT 'DEBUG: CurrentNroSocioAsociadoInvitado = ''' + ISNULL(@CurrentNroSocioAsociadoInvitado, 'NULL') + '''';
            PRINT 'DEBUG: NroSocio (Responsible Account) = ''' + ISNULL(@NroSocio, 'NULL') + '''';
            PRINT 'DEBUG: FecTemporadaCalculada = ''' + ISNULL(CONVERT(VARCHAR(10), @FecTemporadaCalculada, 120), 'NULL') + '''';
            PRINT 'DEBUG: PasePiletaTarifaSocio = ' + CAST(@PasePiletaTarifaSocio AS VARCHAR(20));
            PRINT 'DEBUG: PasePiletaTarifaInvitado = ' + CAST(@PasePiletaTarifaInvitado AS VARCHAR(20));


            IF @EsFacturaDePileta = 1
            BEGIN
                -- Execute the procedure to enable/insert the pool pass.
                EXEC ddbbaTP.HabilitarPasePileta
                    @TarifaSocio = @PasePiletaTarifaSocio,
                    @TarifaInvitado = @PasePiletaTarifaInvitado,
                    @NroSocio = @CurrentNroSocioAsociadoInvitado, -- Will be the sponsor's NroSocio or the member's own
                    @IdInvitado = @CurrentIdInvitado,              -- Will be IdInvitado if applicable, or NULL for members
                    @Fec_Temporada = @FecTemporadaCalculada;       -- Pass the variable with the calculated date

                PRINT 'Pool pass processed after invoice payment.';
            END;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @Msg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Msg, 16, 1);
    END CATCH
END;
GO

------------------------------Actalizar factura

CREATE OR ALTER PROCEDURE ddbbaTP.ModificarFactura
    @IdFactura INT,
    @FechaVencimiento VARCHAR(10) = NULL, -- Nueva fecha de vencimiento (opcional)
    @DiasAtrasados INT = NULL,            -- Nuevos días de atraso (opcional)
    @Estado VARCHAR(20) = NULL,           -- Nuevo estado de la factura (opcional)
    @IdDescuento INT = NULL,              -- Nuevo IdDescuento (opcional)
    @IdCuota INT = NULL,                  -- Nuevo IdCuota (opcional)
    @MontoTotal DECIMAL(10,2) = NULL,     -- Nuevo Monto_Total (opcional)
    @Detalle VARCHAR(255) = NULL,         -- Nuevo Detalle de la factura (opcional)
    @FechaEmision VARCHAR(10) = NULL      -- Nueva Fecha_Emision (opcional)
AS
BEGIN
    --SET NOCOUNT ON;  Evita que se devuelvan recuentos de filas afectados por cada sentencia.
 
    -- Declarar variable para verificar la existencia de la factura
    DECLARE @FacturaExiste BIT;
    DECLARE @MensajeError NVARCHAR(4000);
 
    BEGIN TRY
        -- 1. Verificar si la factura existe
        SELECT @FacturaExiste = 1
        FROM ddbbaTP.Factura
        WHERE IdFactura = @IdFactura;
 
        IF @FacturaExiste IS NULL
        BEGIN
            THROW 50000, 'Error: La factura con el IdFactura especificado no existe.', 1;
        END;
 
        -- 2. Validar FKs si se proporcionan nuevos valores
        IF @IdDescuento IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ddbbaTP.Descuento WHERE IdDescuento = @IdDescuento)
        BEGIN
            SET @MensajeError = 'Error: El IdDescuento proporcionado (' + CAST(@IdDescuento AS NVARCHAR(10)) + ') no existe.';
            THROW 50000, @MensajeError, 1;
        END;
 
        IF @IdCuota IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ddbbaTP.Cuota WHERE IdCuota = @IdCuota)
        BEGIN
            SET @MensajeError = 'Error: El IdCuota proporcionado (' + CAST(@IdCuota AS NVARCHAR(10)) + ') no existe.';
            THROW 50000, @MensajeError, 1;
        END;
 
        -- Iniciar transacción para asegurar atomicidad de la actualización
        BEGIN TRANSACTION;
 
        -- 3. Realizar la actualización de la factura
        UPDATE ddbbaTP.Factura
        SET
            Fecha_Vencimiento = ISNULL(@FechaVencimiento, Fecha_Vencimiento),
            Dias_Atrasados = ISNULL(@DiasAtrasados, Dias_Atrasados),
            Estado = ISNULL(@Estado, Estado),
            IdDescuento = ISNULL(@IdDescuento, IdDescuento),
            IdCuota = ISNULL(@IdCuota, IdCuota),
            Monto_Total = ISNULL(@MontoTotal, Monto_Total),
            Detalle = ISNULL(@Detalle, Detalle),
            Fecha_Emision = ISNULL(@FechaEmision, Fecha_Emision) -- Se agregó el campo Fecha_Emision
        WHERE
            IdFactura = @IdFactura;
 
        COMMIT TRANSACTION; -- Confirmar los cambios si todo fue bien
 
        PRINT 'Factura con IdFactura ' + CAST(@IdFactura AS NVARCHAR(10)) + ' modificada exitosamente.';
 
    END TRY
    BEGIN CATCH
        -- En caso de error, revertir la transacción si está activa
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
 
        -- Capturar y relanzar el error para que la aplicación llamante lo maneje
        SET @MensajeError = ERROR_MESSAGE();
        RAISERROR(@MensajeError, 16, 1);
    END CATCH
END;
go