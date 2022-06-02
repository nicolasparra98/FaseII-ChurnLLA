-----E18-----E19-----E20-----

WITH check_tech as (
Select cast(concat(substr(cast(dt as varchar),1,4),substr(cast(dt as varchar),6,2),substr(cast(dt as varchar),9,2),act_acct_cd) as varchar) as Key_dna , act_acct_cd,

    Case When pd_bb_accs_media = 'FTTH' Then '1. FTTH'
        When pd_bb_accs_media = 'HFC' Then '2. HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then '1. FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then '2. HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '1. FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then '2. HFC'
    ELSE '3. Copper' end as Technology
    
FROM "db-analytics-dev"."dna_fixed_cwp"
)
,max_record as (
Select  
MAX(cast(concat(substr(cast(dt as varchar),1,4),substr(cast(dt as varchar),6,2),substr(cast(dt as varchar),9,2),act_acct_cd) as varchar)) as Key_max
from "db-analytics-dev"."dna_fixed_cwp"
WHERE act_acct_typ_grp IS NOT NULL
GROUP BY act_acct_cd
)
,Join_DNA as (
SELECT a.*, b.*
FROM max_record AS a
LEFT JOIN check_tech AS b
    ON a.Key_max = b.Key_dna
)
,clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME as varchar) != ' ') AND(INTERACTION_START_TIME IS NOT NULL)     AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions as (
    select *,
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
SELECT
ACCOUNT_ID, interaction_id, INTERACTION_DATE, dt
FROM INTERACTIONS
--WHERE INTERACTION_PURPOSE_DESCRIP = 'TICKET' OR INTERACTION_PURPOSE_DESCRIP = 'CLAIM' OR INTERACTION_PURPOSE_DESCRIP = 'CLAIM' OR INTERACTION_PURPOSE_DESCRIP = 'CLAIM'
--Por qué sólo tickets?
--GROUP BY ACCOUNT_ID, interaction_id, INTERACTION_DATE, dt
) 
,Join_Filter_tech as (
Select
a.*,
b.*
FROM Tickets_per_month AS a
LEFT JOIN Join_DNA AS b
    ON a.ACCOUNT_ID = b.act_acct_cd
)
,Last_Interaction as(
Select *, date_trunc('day',date(dt)) as d_t,
FIRST_VALUE(interaction_date) over(partition by ACCOUNT_ID, INTERACTION_DATE order by INTERACTION_DATE desc) as last_interaction_date
From Join_Filter_tech 
)
,Window_day as(
select Distinct account_id as act_accd_ac , *, date_add('DAY',-60, last_interaction_date) as window_day
from Last_Interaction
)
,Day_Filter as (
SELECT
count(interaction_id) as Interactions, act_accd_ac
--, month
from Window_day
Where date(d_t) between window_day and last_interaction_date
group by 2
)
,Join_Last_Interaction as(
select
a.*,b.*
From Last_Interaction as a Join Day_Filter as b on a.account_id = b.act_accd_ac 
)
,User_Interactions as (
SELECT DISTINCT date_trunc('month',CAST(d_t AS DATE)) as MONTH ,ACCOUNT_ID AS USERS
,COUNT(Interactions) AS CALLS
, Technology
FROM Join_Last_Interaction
--WHERE Technology IS NOT Null
GROUP BY 1, ACCOUNT_ID, Technology
order by ACCOUNT_ID 
)
,Filter_KPIs as (
SELECT *,
CASE 
WHEN CALLS = 1 THEN '1' 
WHEN CALLS = 2 THEN '2' 
WHEN CALLS >= 3 THEN '=>3'
ELSE NULL END AS INTERACTIONS
FROM User_Interactions
)

SELECT 
DISTINCT MONTH, INTERACTIONS,
COUNT(DISTINCT USERS) AS USERS
FROM Filter_KPIs
GROUP BY MONTH, INTERACTIONS
ORDER BY MONTH desc, interactions 

------Prueba base usuarios------------
/*with query1 as (
Select act_acct_cd, date_trunc('month', load_dt) as month, act_cust_typ_nm, fi_outst_age
from "db-analytics-dev"."dna_fixed_cwp"   )
,query2 as(
select *
from query1
where date(month) = date('2022-02-01') and act_cust_typ_nm = 'Residencial' 
and ((fi_outst_age <=90)  or (fi_outst_age is null))
)
select count (distinct(act_acct_cd))
from query2 
*/


-----Prueba llamadas usuarios-----------
/* with query1 as (
Select customer_id, date_trunc('month',date(dt)) as month, INTERACTION_START_TIME, INTERACTION_ID
from "db-stage-prod"."interactions_cwp" )
select count(distinct(customer_id)) from query1 
where month = date('2022-02-01') 
and (cast(INTERACTION_START_TIME as varchar) != ' ') AND(INTERACTION_START_TIME IS NOT NULL)     AND INTERACTION_ID NOT LIKE '%-%' and customer_id IS NOT NULL
*/

/*Select count(interaction_id), date_trunc('month', interaction_start_time) as month
from "db-stage-prod"."interactions_cwp" group by 2 order by 2 desc*/
