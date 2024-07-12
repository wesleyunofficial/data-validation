SELECT  *
FROM  brewdat_uc_saz_prod.slv_saz_sales_redemateus.br_sellout
WHERE 1=1
AND DATA == '2024-06-01' 
AND DESCPRODUTO == 'OUTROS 1L'
AND CNPJ == '3995515017647;

SELECT  *
FROM  brewdat_uc_saz_prod.gld_saz_sales_rede_mateus.sellout
WHERE 1=1
AND DATA == '2024-06-01' 
AND DESCPRODUTO == 'OUTROS 1L'
AND CNPJ == '3995515017647';
