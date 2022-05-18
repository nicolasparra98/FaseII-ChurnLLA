WITH
Base AS(
SELECT DISTINCT 
CONCAT(UPPER(SUBSTRING(ACT_PRVNC_CD,1,1)),LOWER(SUBSTRING(ACT_PRVNC_CD,2))) AS Provincia
,CONCAT(UPPER(SUBSTRING(ACT_CANTON_CD,1,1)),LOWER(SUBSTRING(ACT_CANTON_CD,2))) AS Canton
,CONCAT(UPPER(SUBSTRING(ACT_RGN_CD,1,1)),LOWER(SUBSTRING(ACT_RGN_CD,2))) AS Distrito
,CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV
,Tipo_Tecnologia
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` d
 LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` c
  ON PD_BB_PROD_nm=ActivoInternet
GROUP BY 1,2,3,4,5
)
,Technology AS(
SELECT DISTINCT Provincia,Canton,Distrito
,CONCAT(Provincia,Canton,Distrito) AS Llave
,CASE 
  WHEN Tipo_Tecnologia IS NOT NULL THEN Tipo_Tecnologia
  WHEN Tipo_Tecnologia IS NULL AND safe_cast(RGU_TV AS string)="NEXTGEN TV" THEN "FTTH"
  ELSE "HFC" 
END AS TechFlag
FROM Base
GROUP BY 1,2,3,TechFlag
)
SELECT DISTINCT Provincia,Canton,Distrito
,first_value(TechFlag) over(partition by Llave order by TechFlag)
FROM Technology
order by 1,2,3
