-----H1-----H2-----H3-----
WITH 
check_tech as (
Select distinct date_trunc('month',date(load_dt)) as Month, date(load_dt) as load_dt, act_acct_cd,
    Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology
FROM "lla_cco_int_stg"."cwp_fix_union_dna"
WHERE act_cust_typ_nm = 'Residencial'
)
,clean_interaction_time as (
select distinct *
from "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME as varchar) != ' ') AND(INTERACTION_START_TIME IS NOT NULL)     AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions as (
select distinct *,
    CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE) AS INTERACTION_DATE, 
    DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) AS month,
    CASE
    WHEN OTHER_INTERACTION_INFO4 Like '%Retencion%' THEN 'Retention'
    WHEN OTHER_INTERACTION_INFO4 Like 'Retencion ' THEN 'Retention'
    WHEN OTHER_INTERACTION_INFO4 Like 'RETENCION DE CLIENTES ' THEN 'Retention'
    WHEN OTHER_INTERACTION_INFO5 Like 'RETENCION DE CLIENTES' THEN 'Retention'
    WHEN INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' or INTERACTION_PURPOSE_DESCRIP = 'TICKET' THEN 'Technical'
    WHEN INTERACTION_PURPOSE_DESCRIP = 'CLAIM'
        AND OTHER_INTERACTION_INFO5 IN (
        'ADSL Ethernet                                     ',
		'ADSL Smart Home                                   ',
		'ADSL WiFi                                         ',
		'Agrerar y Eliminar MAC                            ',
		'Ajuste                                            ',
		'Ajuste                                            ',
		'Ajuste                                            ',
		'Asignaciones                                      ',
		'Caller ID                                         ',
		'Cambio de Velocidad                               ',
		'Cita incumplida                                   ',
		'Conferencia                                       ',
		'Configuracion Correos                             ',
		'Correccion de Profile                             ',
		'Correo de voz                                     ',
		'DATOS                                             ',
		'Dano Masivo                                       ',
		'Danos                                             ',
		'Danos recurrentes                                 ',
		'Danos recurrentes                                 ',
		'Danos recurrentes                                 ',
		'Desac Correo Voz                                  ',
		'Despertador                                       ',
		'Dunning  Action                                   ',
		'Dunning  Action                                   ',
		'Dunning  Action                                   ',
		'Equipo recuperado                                 ',
		'Errores Cross Connection                          ',
		'FIS                                               ',
		'FSR DATOS E INTERNET                              ',
		'Fuera de meta                                     ',
		'INTERNET                                          ',
		'IP No Disponible                                  ',
		'IVR tiquetes                                      ',
		'IVR tiquetes                                      ',
		'Infructuosa temas Tec                             ',
		'Instalacion demora                                ',
		'Instalacion demora                                ',
		'Instalacion demora                                ',
		'Instalacion estetica                              ',
		'Instalacion estetica                              ',
		'Instalacion estetica                              ',
		'Instalacion incompleta                            ',
		'Instalacion incompleta                            ',
		'Instalacion incompleta                            ',
		'Instalaciones                                     ',
		'Intermitencia                                     ',
		'Intermitencia                                     ',
		'Intermitencia                                     ',
		'Internet                                          ',
		'Internet Cobre                                    ',
		'Internet HFC                                      ',
		'Lenguaje Audio Pixelacion                         ',
		'Lenguaje Audio Pixelacion                         ',
		'Lenguaje Audio Pixelacion                         ',
		'Linea Cobre                                       ',
		'Linea Digital                                     ',
		'Llamada en espera                                 ',
		'MASIVOS                                           ',
		'Mala atencion Area tecnica                        ',
		'Mala atencion Area tecnica                        ',
		'Mala atencion Area tecnica                        ',
		'Masivo                                            ',
		'Masivo                                            ',
		'Masivo                                            ',
		'No tiene servicio                                 ',
		'No tiene servicio                                 ',
		'No tiene servicio                                 ',
		'Numero en pantalla                                ',
		'Paquete de Seguridad                              ',
		'Plantillas Duplicadas                             ',
		'Programacion de                                   ',
		'QUEJA DE INFRAESTRUCTURA                          ',
		'RXC                                               ',
		'Reclamos                                          ',
		'Redes LAN y WAN                                   ',
		'Reparacion demora                                 ',
		'Reparacion demora                                 ',
		'Reparacion demora                                 ',
		'Reparacion estetica                               ',
		'Reparacion estetica                               ',
		'Reparacion estetica                               ',
		'Reparacion incompleta                             ',
		'Reparacion incompleta                             ',
		'Reparacion incompleta                             ',
		'Robo area tecnica                                 ',
		'Robo area tecnica                                 ',
		'Robo area tecnica                                 ',
		'Ruido Linea cruzada                               ',
		'Ruido Linea cruzada                               ',
		'Ruido Linea cruzada                               ',
		'Smart Home                                        ',
		'Smart Security                                    ',
		'Soporte Internet                                  ',
		'Soporte Linea                                     ',
		'Soporte TV                                        ',
		'TV DTH                                            ',
		'TV Master                                         ',
		'TV Motorola                                       ',
		'TV Nagra                                          ',
		'Transferencia                                     ',
		'WiFi                                              ',
		'WiFi                                              ',
		'WiFi                                              ',
		'DAÃ‘OS                                             ',
		'DAÃ‘OS                                             ',
		'DaÃ±o Masivo                                       ',
		'ESCALAMIENTO DAÃ‘OS/TECNICOS                       ',
		'DAï¿½OS   ',
		'Daï¿½o Masivo ',
		'DAï¿½OS',
		'Daï¿½o Masivo',
        'Escalamiento 2 Niv                                ',
        'Escalamiento 3er Nivel                            ',
        'Escalamiento Da os / T cnicos                    ',
        'Reclamos TV                                       ',
        'ACTIVACION DE PRODUCTO                            ',
        'Da o Masivo                                       ',
        'INSTALACION                                       ',
        'Quejas de Instal. F sicas                         ',
        'Verificaci n/ servicio no funciona                ',
        'Fuera de Meta                                     ',
        'IRREGULARIDAD EN SERVICIO                         ',
        'Dentro meta                                       ',
        'Dentro de Meta                                    ',
        'DAÑOS                                             '
        ) THEN 'Technical'
    WHEN OTHER_INTERACTION_INFO5 IN(
        'Consulta de saldo                                 ',
        'Cliente restringido                               ',
        'Consulta de reclamo                               ',
        'Consulta de cuentas                               ',
        'INFORMACION GENERAL                               ',
        'Cliente desconectado                              ',
        'Consulta de productos y servicios                 ',
        'Consulta Prorrateo                                ',
        'INTERNACIONALES (MADI)                            ',
        'IVR saldo                                         ',
        'NACIONALES                                        ',
        'LOCALES                                           ',
        'CELULARES                                         ',
        'Productos LDI                                     ',
        'Productos LDN                                     ',
        'Consulta de saldo                                 ',
        'IVR saldo                                         '
    ) THEN 'Account Info'
    WHEN OTHER_INTERACTION_INFO5 IN(
        'Facturacion                                       ',
        'Consulta arreglo de pago                          ',
        'CARGO NO APLICA                                   ',
        'TARIFA O CARGO FIJO                               ',
        'Facturacion                                       ',
        'Afiliacion Facturacion Web                        ',
        'Como pagar On Line                                ',
        'No entiende la Factura                            ',
        'PAGOS EN AGENCIA EXTERNA                          ',
        'DEVOLUCIONES DE CREDITO                           ',
        'Facturacion Web                                   ',
        'Pago no posteado                                  ',
        'CTA. FINAL IRREGULAR                              ',
        'Pagos  Recargas                                   ',
        'DESCUENTO DE JUBILADO                             ',
        'No entiende la Factura                            ',
        'PAGO CWP MAL REGISTRADO                           ',
        'INVEST. ALTO CONSUMO                              ',
        'Promociones o Precios                             ',
        'PAGOS EN AGENCIA EXTERNA                          ',
        'PROMOCIONES                                       ',
        'FSR RES.1ER.CONT.POSTPAGO                         ',
        'IMPRESION DE FACTURA                              ',
        'PLANES PROMOCIONALES                              ',
        'INVEST. ALTO CONSUMO                              ',
        'Pagos  Recargas                                   ',
        'TRANSFERENCIA DE SALDO                            ',
        'ANUNCIO DEL DIRECTO                               '      
    ) THEN 'Billing' 
    ELSE 'Others'
    END AS INTERACTION_TYPE
FROM clean_interaction_time
)
,Tickets_per_month as (
select distinct ACCOUNT_ID, interaction_id, INTERACTION_DATE
FROM interactions
where interaction_purpose_descrip = 'TICKET'
) 
,Last_Interaction as(
Select distinct account_id as last_account
,first_value(interaction_date) over(partition by account_id,date_trunc('month',interaction_date) order by interaction_date desc) as last_interaction_date
From Tickets_per_month
)
,Join_Last_Interaction as(
select distinct account_id,interaction_id,interaction_date,date_trunc('month',last_interaction_date) as InteractionMonth,last_interaction_date,date_add('DAY',-60, last_interaction_date) as window_day
from Tickets_per_month w inner join Last_Interaction l on w.account_id=l.last_account
)
,Interactions_Count as (
select distinct InteractionMonth,account_id,count(interaction_id) as Interactions
from Join_Last_Interaction
where interaction_date between window_day and last_interaction_date
group by 1,2
)
,Filter_KPIs as (
select distinct i.*,
case when Interactions = 1 THEN '1' 
     when Interactions = 2 THEN '2' 
     when Interactions >= 3 THEN '>3'
else null end as InteractionsTier
FROM Interactions_Count i --INNER JOIN Check_Tech c ON c.act_acct_cd=i.account_id AND c.Month=i.InteractionMonth
)

select distinct interactionmonth as Month,InteractionsTier,COUNT(DISTINCT account_id) AS Records
from Filter_KPIs
where interactionmonth>=date('2021-10-01')
group by 1,2
order by 1,2

