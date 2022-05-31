SELECT DISTINCT CHANNEL_DESC,
CASE WHEN Channel_desc IN ('Provincia de Chiriqui','PROM','VTASE','PHs1','Busitos','Alianza','Coronado','Ventas Externas/ADSL','PHs 2') OR Channel_desc LIKE '%PROM%' OR Channel_desc LIKE '%VTASE%' OR Channel_desc LIKE '%Busitos%' OR Channel_desc LIKE '%Alianza%' THEN 'D2D (Own Sales force)'
    WHEN Channel_desc IN('Dinamo','Oficinista','Distribuidora Arandele','Orde Technology','SLAND','SI Panamá') THEN 'D2D (Outsourcing)'
    WHEN Channel_desc IN('Vendedores','Metro Mall','WESTLAND MALL','TELEMART AGUADULCE') THEN 'Retail (Own Stores)'
    WHEN Channel_desc IN(/*'Telefono',*/'123 Outbound','Gestión') OR Channel_desc LIKE '%Gestión%' OR Channel_desc LIKE '%Gestion%' THEN 'Outbound – TeleSales'
    WHEN Channel_desc IN('Centro de Retencion','Centro de Llamadas','Call Cnter MULTICALL') THEN 'Inbound – TeleSales'
    WHEN Channel_desc IN('Nestrix','Tienda OnLine','Live Person','Telefono') THEN 'Digital'
    WHEN Channel_desc IN('Panafoto Dorado','Agencia') OR Channel_desc LIKE '%Agencia%' OR Channel_desc LIKE '%AGENCIA%' THEN 'Retail (Distributer-Dealer)'
    WHEN Channel_desc IN('CIS+ GUI','Solo para uso de IT','Apuntate',' CU2Si','RC0E Collection','Carta','Proyecto','DE=Demo','Recarga saldo','Port Postventa','Feria','Administracion','Postventa-verif.orde','No Factibilidad','Orden a construir','Inversiones AP','Promotor','VIVI MAS') OR Channel_desc LIKE '%Feria%' THEN 'Not a Sales Channel'
END AS Sales_Channel_SO
,COUNT(DISTINCT ACCOUNT_ID) AS RECORDS
from (SELECT DISTINCT date_trunc('Month', date(completed_date)) AS Month, account_id, first_value(channel_desc) over (partition by account_id order by order_start_date) as channel_desc
    FROM "db-analytics-prod"."so_hdr_cwp")
GROUP BY 1,2
ORDER BY 2,RECORDS DESC
