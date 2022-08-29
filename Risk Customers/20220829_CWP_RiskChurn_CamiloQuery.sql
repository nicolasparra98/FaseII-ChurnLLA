--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_PruebaDt" AS
with parameters as (
select
--##############################################################
--### Change Date in this line to define paying period #########
--DATE_ADD('day', -30, CURRENT_DATE) as interactions_start_range,
date('2022-07-01') as interactions_start_range,
date('2022-07-31') as interactions_end_range
--CURRENT_DATE as interactions_end_range
--date('2022-07-01') as interactions_start_range,
--date('2022-07-31') as 

--##############################################################
),

interactions as (
select ACCOUNT_ID, interaction_id, interaction_purpose_descrip,interaction_start_time,other_interaction_info4, other_interaction_info5,
    date(INTERACTION_START_TIME) as interaction_start_date,
    interaction_agent_name, interaction_channel as interaction_channel_original,
    CASE WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_agent_name LIKE '%TP%' THEN 'claims_TP'
        WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_agent_name NOT LIKE '%TP%' THEN 'claims_Others'
        else interaction_channel end as interaction_channel_class,
    CASE WHEN interaction_purpose_descrip = 'CLAIM' AND lower(other_interaction_info4) Like '%retencion%' THEN 'retention_claim'
        WHEN interaction_purpose_descrip = 'CLAIM' AND other_interaction_info5 IN (
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
            'DA�OS                                             ',
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
            ) THEN 'technical_claim'
        WHEN interaction_purpose_descrip = 'CLAIM' AND other_interaction_info5 IN(
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
        ) THEN 'account_info_or_balance_claim'
        WHEN interaction_purpose_descrip = 'CLAIM' AND other_interaction_info5 IN(
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
        ) THEN 'billing_claim'
        WHEN interaction_purpose_descrip = 'CLAIM' then 'other_claims'
        WHEN interaction_purpose_descrip = 'TICKET' then 'tech_ticket'
        WHEN interaction_purpose_descrip = 'TRUCKROLL' then 'tech_truckroll'
        END AS interact_category,
    concat(other_interaction_info4,'-',other_interaction_info5,'-',other_interaction_info6,'-',other_interaction_info8) as disposition_info,
    first_value(other_interaction_info8) over (partition by interaction_id order by cast(data_creation_timestamp as timestamp) desc) as last_teck_disponsition
    from "db-stage-prod"."interactions_cwp"
    where (date(INTERACTION_START_TIME) between (select interactions_start_range from parameters) and (select interactions_end_range from parameters))
    and interaction_id is not null 
    --and interaction_purpose_descrip = 'TRUCKROLL' or interaction_purpose_descrip = 'TICKET'
    --and interaction_status = 'CLOSED' AND interaction_id NOT LIKE '%1-%'
    order by interaction_start_date
),
interactions_panel as (
select ACCOUNT_ID as interactions_account_id, 
--#### TOTAL INTERACTIONS ####
count(distinct interaction_start_date) as num_distinct_interact_days_all,
filter(array_sort(array_agg(distinct interaction_start_date)), x -> x IS NOT NULL) as list_interaction_dates_all,
filter(array_sort(array_agg(distinct interaction_id)), x -> x IS NOT NULL) as list_interaction_ids_all,
filter(array_sort(array_agg(distinct interaction_channel_class)), x -> x IS NOT NULL) as list_interaction_channels_class_all,
filter(array_sort(array_agg(distinct interact_category)), x -> x IS NOT NULL) as list_interact_category_all,
filter(array_sort(array_agg(distinct disposition_info)), x -> x IS NOT NULL) as list_disposition_info_all,
--#### CLAIMS TP + TECH TICKETS ####
count(distinct (case when interaction_channel_class <> 'claims_Others' then interaction_start_date end)) as num_distinct_interact_days_tp_claims_tckt_tr,
filter(array_sort(array_agg(distinct (case when interaction_channel_class <> 'claims_Others' then interaction_start_date end))), x -> x IS NOT NULL) as list_interaction_dates_tp_claims_tckt_tr,
filter(array_sort(array_agg(distinct (case when interaction_channel_class <> 'claims_Others' then interaction_id end))), x -> x IS NOT NULL) as list_interaction_ids_tp_claims_tckt_tr,
filter(array_sort(array_agg(distinct (case when interaction_channel_class <> 'claims_Others' then interaction_channel_class end))), x -> x IS NOT NULL) as list_interaction_channels_class_tp_claims_tckt_tr,
filter(array_sort(array_agg(distinct (case when interaction_channel_class <> 'claims_Others' then interact_category end))), x -> x IS NOT NULL) as list_interact_category_tp_claims_tckt_tr,

--#### CLAIMS TP + TECH TICKETS + EXCLUDE BALANCE AND ACCT_INFO ####
count(distinct (case when (interaction_channel_class <> 'claims_Others' AND interact_category <> 'account_info_or_balance_claim') then interaction_start_date end)) as num_distinct_interact_days_tp_claims_tckt_tr_excl_bal_acct,
filter(array_sort(array_agg(distinct (case when (interaction_channel_class <> 'claims_Others' AND interact_category <> 'account_info_or_balance_claim') then interaction_start_date end))), x -> x IS NOT NULL) as list_interaction_dates_tp_claims_tckt_tr_excl_bal_acct,
filter(array_sort(array_agg(distinct (case when (interaction_channel_class <> 'claims_Others' AND interact_category <> 'account_info_or_balance_claim') then interaction_id end))), x -> x IS NOT NULL) as list_interaction_ids_tp_claims_tckt_tr_excl_bal_acct,
filter(array_sort(array_agg(distinct (case when (interaction_channel_class <> 'claims_Others' AND interact_category <> 'account_info_or_balance_claim') then interaction_channel_class end))), x -> x IS NOT NULL) as list_interaction_channels_class_tp_claims_tckt_tr_excl_bal_acct,
filter(array_sort(array_agg(distinct (case when (interaction_channel_class <> 'claims_Others' AND interact_category <> 'account_info_or_balance_claim') then interact_category end))), x -> x IS NOT NULL) as list_interact_category_tp_claims_tckt_tr_excl_bal_acct

from interactions
group by ACCOUNT_ID
),

SIR_model as (
Select*
FROM "db-stage-prod"."scores_001_cwp"
WHERE date(year_month) = date('2022-07-05')
),

join_SIR_Interactions as (
SELECT
a.*,
b.num_distinct_interact_days_all
FROM SIR_model as a
LEFT JOIN interactions_panel as b ON CAST(a.act_acct_cd as VARCHAR) = CAST(b.interactions_account_id as VARCHAR)
)
select distinct date(year_month) as year_month,risk_customer,count(distinct act_acct_cd)
from(
SELECT *,
CASE WHEN percentile_rank_score >=90 OR num_distinct_interact_days_all >= 4 THEN 'Alto_riesgo' ELSE 'Riesgo_medio'
END AS Risk_customer,
CASE WHEN score =1 THEN 'tratamiento' When score =-1 THEN 'control' ELSE 'otros' END AS Customer_Group
FROM join_SIR_Interactions
) group by 1,2 order by 1,2
