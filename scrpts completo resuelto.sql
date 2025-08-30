-- ================================================================
-- BD Jardinería – Fase 3: DECIDIR / ORGANIZAR (Guion inicial)
-- ================================================================
-- Notas:
-- - INNER JOIN para coincidencias obligatorias
-- - LEFT JOIN para detectar ausencias (NULL del lado derecho)
-- - IN para listas/valores concretos; EXISTS/NOT EXISTS para correlación y manejo de NULLs
-- - Vistas bajo esquema dbo con prefijo vw_
-- - Transacciones con TRY/CATCH y SET XACT_ABORT ON recomendado
-- ================================================================


-- ======================
-- [JOINs] (10 consultas)
-- ======================

-- J1 [INNER]: Clientes y sus representantes (clientes ↔ empleados)
select * from cliente c 
inner join empleado e 
on c.codigo_empleado_rep_ventas = e.codigo_empleado


-- J2 [LEFT]: Clientes con total pagado (incluir clientes sin pagos)
select * from cliente c 
left join pago p
on c.codigo_cliente = p.codigo_cliente

-- J3 [INNER]: Pedidos por cliente con estado y fechas
select c.codigo_cliente,c.nombre_cliente,p.codigo_pedido,p.fecha_pedido,p.fecha_entrega from pedido p 
inner join cliente c 
on c.codigo_cliente = p.codigo_cliente

-- J4 [INNER]: Productos y su gama (productos ↔ gamas_productos)
select p.codigo_producto,p.nombre,p.gama,gp.descripcion_texto  from [dbo].[producto] p 
inner join [dbo].[gama_producto] gp 
on p.gama = gp.gama

select * from [dbo].[gama_producto]


-- J5 [LEFT]: Ingreso por producto (detalle_pedidos puede ser nulo)
select * from [dbo].[detalle_pedido] dp
left join [dbo].[producto] p 
on dp.codigo_producto = p.codigo_producto 


-- J6 [LEFT]: Oficinas y número de empleados (incluir oficinas sin empleados)
select 
    o.codigo_oficina,
    count(e.codigo_empleado) as numero_empleados
from [dbo].[oficina] o
left join [dbo].[empleado] e
    on o.codigo_oficina = e.codigo_oficina
group by o.codigo_oficina;



-- J7 [LEFT]: Empleados y su jefe (auto-relación empleados)
select 
    e1.codigo_empleado,
    concat(e1.nombre,' ',e1.apellido1) as Nombre_empleado,
    e2.codigo_empleado as codigo_jefe,
    concat(e2.nombre,' ',e2.apellido1) as Nombre_jefe
from [dbo].[empleado] e1
left join [dbo].[empleado] e2
    on e1.codigo_jefe = e2.codigo_empleado;


-- J8 [LEFT]: Clientes sin pedidos (filtrar p.codigo_pedido IS NULL)
select c.codigo_cliente,c.nombre_cliente,count(p.codigo_pedido) from [dbo].[cliente] c 
left join [dbo].[pedido] p 
on c.codigo_cliente = p.codigo_cliente
where p.codigo_pedido is null 
group by c.codigo_cliente,c.nombre_cliente


-- J9 [LEFT]: Productos sin ventas (filtrar dp.codigo_pedido IS NULL)
select p.codigo_producto,p.nombre, count(pe.codigo_pedido) as cantidad_pedidios
from [dbo].[producto] p
left join [dbo].[detalle_pedido] pe 
on pe.codigo_producto = p.codigo_producto  
where pe.codigo_pedido is null
group by p.codigo_producto, p.nombre


-- J10 [LEFT]: Ingreso por gama (agregado por pr.gama)
select prod.gama,sum((dp.cantidad*dp.precio_unidad)) as Total from [dbo].[gama_producto] prod
left join [dbo].[producto] p 
on prod.gama = p.gama
left join [dbo].[detalle_pedido] dp 
on p.codigo_producto = dp.codigo_producto
group by prod.gama




-- ==========================
-- [SUBCONSULTAS] (10 items)
-- ==========================

-- S1 [IN]: Clientes con al menos un pedido
SELECT c.codigo_cliente,c.nombre_cliente from cliente c
where codigo_cliente IN(
    select codigo_cliente 
    from pedido p
    )



-- S2 [NOT IN]: Clientes sin pagos (teniendo cuidado con NULLs)
SELECT c.codigo_cliente,c.nombre_cliente from cliente c
where codigo_cliente NOT IN(
    select codigo_cliente 
    from pago p
    where codigo_cliente is not null
    )

-- S3 [EXISTS]: Clientes con pedidos en estado 'Entregado'
SELECT nombre_cliente
from cliente c
WHERE EXISTS(
    SELECT codigo_cliente 
    FROM pedido p 
    WHERE p.codigo_cliente= c.codigo_cliente
    AND p.estado = 'Entregado'
);



-- S4 [Correlacionada]: Productos > precio medio de su propia gama Para el caso S4 [Correlacionada], queremos listar los productos cuyo precio de venta es mayor que el precio promedio de los productos de su misma gama.
-- S4 [Correlacionada]: Productos con precio mayor al promedio de su propia gama
SELECT p.codigo_producto,
       p.nombre,
       p.gama,
       p.precio_venta
FROM producto p
WHERE p.precio_venta > (
    SELECT AVG(p2.precio_venta)
    FROM producto p2
    WHERE p2.gama = p.gama
);



-- S5 [EXISTS]: Empleados que son jefes de alguien
SELECT nombre,codigo_jefe,codigo_empleado FROM empleado e
where EXISTS(
    SELECT 1
     FROM
    empleado e2
    where   e.codigo_empleado = e2.codigo_jefe
);

-- S6 [NOT EXISTS]: Oficinas sin empleados
select * from oficina ofi
WHERE  NOT EXISTS(
    select codigo_oficina
    FROM empleado e
   WHERE e.codigo_oficina = ofi.codigo_oficina
)
-- S7 [Correlacionada]: Clientes con límite de crédito < total de sus pedidos
SELECT * from cliente
-- S8 [IN/Correlacionada]: Productos con stock < promedio global

-- S9 [EXISTS]: Clientes con pagos > promedio de todos los pagos

-- S10 [IN + TOP]: Pedidos dentro del top 10 por importe total


-- ======================
-- [VISTAS] (5 definidas)
-- ======================

-- V1: dbo.vw_ClientesSinPagos  -- Clientes sin registros en pagos
SELECT  FROM 
pago

-- V2: dbo.vw_OficinasBajaActividad  -- Oficinas con < 3 empleados
-- V3: dbo.vw_RankingVentasProducto  -- Ingreso por producto (agregado)
-- V4: dbo.vw_PedidosPendientesPorCliente  -- Conteo de pedidos 'Pendiente/En Proceso' por cliente
-- V5: dbo.vw_IngresoPorGama  -- Ingreso total por gama


-- =========================================
-- [TRANSACCIONES] (15: 5 INSERT / 5 UPDATE / 5 DELETE)
-- =========================================
-- hacer la captura del rollback y el commit
-- --------
-- INSERT (5)
-- --------
-- T-INS-1: Alta de pedido + 1 detalle con control de stock
-- T-INS-2: Registrar un pago si el cliente tiene pedidos entregados
-- T-INS-3: Crear oficina y dos empleados (SAVEPOINT para revertir parcial)
-- T-INS-4: Alta masiva de productos de una gama nueva (validar existencia)
-- T-INS-5: Clonar cliente como “prospecto” (prefijo en nombre y crédito=0)

-- -------
-- UPDATE (5)
-- -------
-- T-UPD-1: Marcar pedido como 'Entregado' si cumple condición (fecha_entrega)
-- T-UPD-2: Reasignar representante de ventas a un cliente (validar empleado)
-- T-UPD-3: Ajuste de stock tras auditoría (no permitir negativos)
-- T-UPD-4: Incrementar precio de una gama con tope máximo
-- T-UPD-5: Normalización de direcciones (limpiar campos vacíos)

-- -------
-- DELETE (5)
-- -------
-- T-DEL-1: Eliminar detalle de pedido específico; si queda sin detalles, eliminar pedido (SAVEPOINT)
-- T-DEL-2: Borrar pagos duplicados por id_transaccion (CTE + ROW_NUMBER)
-- T-DEL-3: Eliminar productos sin ventas y stock = 0
-- T-DEL-4: Eliminar clientes sin pedidos ni pagos (seguro)
-- T-DEL-5: Eliminar oficina solo si no tiene empleados (validación previa)


-- ================================================================
-- Espacios para el código (dejar preparados los bloques vacíos)
-- ================================================================

-- [Plantilla JOIN]
-- SELECT ... 
-- FROM tablaA a
-- INNER/LEFT JOIN tablaB b ON ...
-- WHERE ...
-- GROUP BY ...
-- ORDER BY ...;


-- [Plantilla SUBCONSULTA IN]
-- SELECT ...
-- FROM ...
-- WHERE columna IN (SELECT ... FROM ... WHERE ...);


-- [Plantilla SUBCONSULTA EXISTS]
-- SELECT ...
-- FROM alias a
-- WHERE EXISTS (SELECT 1 FROM otra o WHERE o.fk = a.pk AND ...);


-- [Plantilla VISTA]
-- DROP VIEW IF EXISTS dbo.vw_NombreVista;
-- GO
-- CREATE VIEW dbo.vw_NombreVista AS
-- SELECT ...
-- FROM ...
-- WHERE ...;
-- GO


-- [Plantilla TRANSACCIÓN]
-- SET XACT_ABORT ON;
-- BEGIN TRY
--   BEGIN TRAN;
--   -- DML aquí (INSERT/UPDATE/DELETE)
--   COMMIT;
-- END TRY
-- BEGIN CATCH
--   IF @@TRANCOUNT > 0 ROLLBACK;
--   -- Opcional: SELECT ERROR_NUMBER() AS Err, ERROR_MESSAGE() AS Msg;
--   THROW;
-- END CATCH;
-- GO
