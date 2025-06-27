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

CREATE OR ALTER PROCEDURE ddbbaTP.Importar_Clima  @RutaArchivo VARCHAR(200)
AS
BEGIN
    CREATE TABLE #carga_Clima_Temp (
        fechaYHora     VARCHAR(30),
        temperatura    VARCHAR(10),
        lluvia         VARCHAR(20),
        humedad        VARCHAR(10),
        viento         VARCHAR(20)
    );

    DECLARE @SQL_Clima NVARCHAR(MAX) = '
        BULK INSERT #carga_Clima_Temp
        FROM ''' + @RutaArchivo + '''
        WITH (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 4,
            CODEPAGE = ''ACP''
        );';

    EXEC sp_executesql @SQL_Clima;
	--select * from #carga_Clima_Temp

	INSERT INTO ddbbaTP.Dia_Lluvia (Fecha)
	SELECT DISTINCT 
		CAST(TRY_CAST(REPLACE(fechaYHora, 'T', ' ') AS DATETIME) AS DATE)
	FROM #carga_Clima_Temp
	WHERE TRY_CAST(REPLACE(lluvia, ',', '.') AS DECIMAL(8,2)) > 0
	  AND TRY_CAST(REPLACE(fechaYHora, 'T', ' ') AS DATETIME) IS NOT NULL
	  AND CAST(TRY_CAST(REPLACE(fechaYHora, 'T', ' ') AS DATETIME) AS DATE) NOT IN (
		  SELECT Fecha FROM ddbbaTP.Dia_Lluvia
	  );

     DROP TABLE #carga_Clima_Temp;
END;
go

EXEC ddbbaTP.Importar_Clima 'C:\_temp\open-meteo-buenosaires_2024.csv' --Guardamos el archivo en extensión .csv UTF-8
go
EXEC ddbbaTP.Importar_Clima 'C:\_temp\open-meteo-buenosaires_2025.csv' --Guardamos el archivo con extensión .csv UTF-8
go

select * from ddbbaTP.Dia_LLuvia
go
