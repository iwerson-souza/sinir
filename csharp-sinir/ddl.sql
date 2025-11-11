-- MySQL 5.7.37 DDL for local SINIR processing
-- Schema: sinir

CREATE TABLE IF NOT EXISTS stakeholder (
  unidade           VARCHAR(32)  NOT NULL,
  cpf_cnpj          VARCHAR(32)  NOT NULL,
  nome              VARCHAR(255) NOT NULL,
  data_inicial      DATE         NULL,
  data_final        DATE         NULL,
  created_by        VARCHAR(64)  NOT NULL,
  created_dt        DATETIME     NOT NULL,
  last_modified_by  VARCHAR(64)  NULL,
  last_modified_dt  DATETIME     NULL,
  PRIMARY KEY (unidade, cpf_cnpj),
  KEY idx_stakeholder_data_final (data_final)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS mtr_load (
  url           VARCHAR(256) NOT NULL,
  unidade       VARCHAR(32)   NOT NULL,
  status        ENUM('PENDING','PROCESSING','DONE','ERROR', 'SPLITTING') NOT NULL DEFAULT 'PENDING',
  created_by    VARCHAR(64)   NOT NULL,
  created_dt    DATETIME      NOT NULL,
  locked_by     VARCHAR(128)  NULL,
  locked_at     DATETIME      NULL,
  last_error    TEXT          NULL,
  PRIMARY KEY (url),
  KEY idx_mtr_load_status_created (status, created_dt),
  KEY idx_mtr_load_unidade (unidade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS mtr (
  numero                 VARCHAR(64)  NOT NULL,
  tipo_manifesto         VARCHAR(64)  NOT NULL,
  responsavel_emissao    VARCHAR(255) NOT NULL,
  tem_mtr_complementar   VARCHAR(64)  NULL,
  numero_mtr_provisorio  VARCHAR(64)  NULL,
  data_emissao           VARCHAR(16)  NOT NULL,
  data_recebimento       VARCHAR(16)  NOT NULL,
  situacao               VARCHAR(64)  NOT NULL,
  responsavel_recebimento VARCHAR(255) NULL,
  justificativa          TEXT         NULL,
  tratamento             VARCHAR(255) NOT NULL,
  numero_cdf             VARCHAR(64)  NULL,
  residuos               JSON         NOT NULL,
  residuos_codigo        VARCHAR(2048) NOT NULL,
  residuos_classe        VARCHAR(2048) NOT NULL,
  gerador                JSON         NOT NULL,
  transportador          JSON         NOT NULL,
  destinador             JSON         NOT NULL,
  gerador_cpf_cnpj       VARCHAR(32)  NOT NULL,
  transportador_cpf_cnpj VARCHAR(32)  NOT NULL,
  destinador_cpf_cnpj    VARCHAR(32)  NOT NULL,
  cpfs_cnpjs             VARCHAR(256) NOT NULL,
  created_by             VARCHAR(64)  NOT NULL,
  created_dt             DATETIME     NOT NULL,
  PRIMARY KEY (numero),
  KEY idx_mtr_gerador (gerador_cpf_cnpj),
  KEY idx_mtr_transportador (transportador_cpf_cnpj),
  KEY idx_mtr_destinador (destinador_cpf_cnpj)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS error (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  source      VARCHAR(64)  NOT NULL,
  reference   VARCHAR(1024) NULL,
  message     TEXT         NOT NULL,
  stack       MEDIUMTEXT   NULL,
  created_dt  DATETIME     NOT NULL,
  extra       JSON         NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Optional helper view to inspect pending load pressure
CREATE OR REPLACE VIEW vw_pending_loads AS
  SELECT unidade, COUNT(*) AS qty
  FROM mtr_load
  WHERE status = 'PENDING'
  GROUP BY unidade
  ORDER BY qty DESC;

CREATE TABLE sinir.`mtr_history` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `numero` varchar(64) NOT NULL,
  `tipo_manifesto` varchar(64) NOT NULL,
  `responsavel_emissao` varchar(255) NOT NULL,
  `tem_mtr_complementar` varchar(64) DEFAULT NULL,
  `numero_mtr_provisorio` varchar(64) DEFAULT NULL,
  `data_emissao` varchar(16) NOT NULL,
  `data_recebimento` varchar(16) NOT NULL,
  `situacao` varchar(64) NOT NULL,
  `responsavel_recebimento` varchar(255) DEFAULT NULL,
  `justificativa` text,
  `tratamento` varchar(255) NOT NULL,
  `numero_cdf` varchar(64) DEFAULT NULL,
  `residuos` json NOT NULL,
  `residuos_codigo` varchar(2048) NOT NULL,
  `residuos_classe` varchar(2048) NOT NULL,
  `gerador` json NOT NULL,
  `transportador` json NOT NULL,
  `destinador` json NOT NULL,
  `gerador_cpf_cnpj` varchar(32) NOT NULL,
  `transportador_cpf_cnpj` varchar(32) NOT NULL,
  `destinador_cpf_cnpj` varchar(32) NOT NULL,
  `cpfs_cnpjs` varchar(256) NOT NULL,
  `created_by` varchar(64) NOT NULL,
  `created_dt` datetime NOT NULL,
  `history_dt` DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY `idx_mtr_gerador` (`gerador_cpf_cnpj`),
  KEY `idx_mtr_transportador` (`transportador_cpf_cnpj`),
  KEY `idx_mtr_destinador` (`destinador_cpf_cnpj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


//---------------------  

delete from error;
delete from mtr;
delete from mtr_load;
delete from stakeholder;

INSERT INTO `sinir`.`mtr_load`
(`url`,
`unidade`,
`status`,
`created_by`,
`created_dt`)
VALUES
('https://mtr.sinir.gov.br/api/mtr/pesquisaManifestoRelatorioMtrAnalitico/97653/18/5/02-10-2025/31-10-2025/8/0/9/0',
'97653',
'PENDING',
'iwerson',
now());

INSERT INTO `sinir`.`stakeholder`
(`unidade`,
`cpf_cnpj`,
`nome`,
`data_inicial`,
`data_final`,
`created_by`,
`created_dt`)
VALUES
('97653',
'37.979.338/0001-47',
'PERFORMA INTELIGENCIA EM SERVICOS AMBIENTAIS LTDA',
null,
null,
'iwerson',
now());

//---------------------  

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_split_mtr_load_drain`(IN p_batch INT)
BEGIN
DECLARE v_moved INT DEFAULT 0;
IF p_batch IS NULL OR p_batch < 1 THEN SET p_batch = 500; END IF;

-- Phase 1: move all eligible PENDING to PROCESSING in batches
REPEAT
UPDATE sinir.mtr_load
SET status = 'SPLITTING'
WHERE status = 'ERROR'
AND YEAR(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -6), '/', 1), '%d-%m-%Y'))
= YEAR(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -5), '/', 1), '%d-%m-%Y'))
AND MONTH(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -6), '/', 1), '%d-%m-%Y'))
= MONTH(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -5), '/', 1), '%d-%m-%Y'))
ORDER BY created_dt
LIMIT p_batch;
SET v_moved = ROW_COUNT();
UNTIL v_moved = 0 END REPEAT;

-- Phase 2: feed batches back to PENDING and split
REPEAT
UPDATE sinir.mtr_load
SET status = 'ERROR'
WHERE status = 'SPLITTING'
AND YEAR(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -6), '/', 1), '%d-%m-%Y'))
= YEAR(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -5), '/', 1), '%d-%m-%Y'))
AND MONTH(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -6), '/', 1), '%d-%m-%Y'))
= MONTH(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', -5), '/', 1), '%d-%m-%Y'))
ORDER BY created_dt
LIMIT p_batch;

SET v_moved = ROW_COUNT();

IF v_moved > 0 THEN
  CALL sinir.sp_split_mtr_load_into_thirds();
END IF;
UNTIL v_moved = 0 END REPEAT;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_split_mtr_load_into_thirds`()
BEGIN
START TRANSACTION;

DROP TEMPORARY TABLE IF EXISTS tmp_mtr_split;

CREATE TEMPORARY TABLE tmp_mtr_split AS
SELECT
m.url,
m.unidade,
m.created_by,
STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -6), '/', 1), '%d-%m-%Y') AS start_dt,
STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -5), '/', 1), '%d-%m-%Y') AS end_dt,
SUBSTRING_INDEX(m.url,
CONCAT('/', SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -6), '/', 1), '/'),
1) AS prefix_part,
SUBSTRING_INDEX(m.url,
CONCAT('/', SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -5), '/', 1), '/'),
-1) AS suffix_part
FROM sinir.mtr_load m
WHERE m.status = 'ERROR'
AND YEAR(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -6), '/', 1), '%d-%m-%Y'))
= YEAR(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -5), '/', 1), '%d-%m-%Y'))
AND MONTH(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -6), '/', 1), '%d-%m-%Y'))
= MONTH(STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(m.url, '/', -5), '/', 1), '%d-%m-%Y'));

-- Window 1: [start .. min(end, start+8)]
INSERT INTO sinir.mtr_load (url, unidade, status, created_by, created_dt)
SELECT
CONCAT(prefix_part, '/',
DATE_FORMAT(start_dt, '%d-%m-%Y'), '/',
DATE_FORMAT(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 8 DAY)), '%d-%m-%Y'),
'/', suffix_part),
unidade,
'PENDING',
created_by,
NOW()
FROM tmp_mtr_split t
WHERE start_dt <= LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 8 DAY))
AND NOT EXISTS (
SELECT 1 FROM sinir.mtr_load m
WHERE m.url = CONCAT(prefix_part, '/',
DATE_FORMAT(start_dt, '%d-%m-%Y'), '/',
DATE_FORMAT(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 8 DAY)), '%d-%m-%Y'),
'/', suffix_part)
);

-- Window 2: [min(end, start+8)+1 .. min(end, start+18)]
INSERT INTO sinir.mtr_load (url, unidade, status, created_by, created_dt)
SELECT
CONCAT(prefix_part, '/',
DATE_FORMAT(DATE_ADD(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 8 DAY)), INTERVAL 1 DAY), '%d-%m-%Y'), '/',
DATE_FORMAT(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 18 DAY)), '%d-%m-%Y'),
'/', suffix_part),
unidade,
'PENDING',
created_by,
NOW()
FROM tmp_mtr_split t
WHERE DATE_ADD(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 8 DAY)), INTERVAL 1 DAY)
<= LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 18 DAY))
AND NOT EXISTS (
SELECT 1 FROM sinir.mtr_load m
WHERE m.url = CONCAT(prefix_part, '/',
DATE_FORMAT(DATE_ADD(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 8 DAY)), INTERVAL 1 DAY), '%d-%m-%Y'), '/',
DATE_FORMAT(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 18 DAY)), '%d-%m-%Y'),
'/', suffix_part)
);

-- Window 3: [min(end, start+18)+1 .. end]
INSERT INTO sinir.mtr_load (url, unidade, status, created_by, created_dt)
SELECT
CONCAT(prefix_part, '/',
DATE_FORMAT(DATE_ADD(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 18 DAY)), INTERVAL 1 DAY), '%d-%m-%Y'), '/',
DATE_FORMAT(end_dt, '%d-%m-%Y'),
'/', suffix_part),
unidade,
'PENDING',
created_by,
NOW()
FROM tmp_mtr_split t
WHERE DATE_ADD(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 18 DAY)), INTERVAL 1 DAY) <= end_dt
AND NOT EXISTS (
SELECT 1 FROM sinir.mtr_load m
WHERE m.url = CONCAT(prefix_part, '/',
DATE_FORMAT(DATE_ADD(LEAST(end_dt, DATE_ADD(start_dt, INTERVAL 18 DAY)), INTERVAL 1 DAY), '%d-%m-%Y'), '/',
DATE_FORMAT(end_dt, '%d-%m-%Y'),
'/', suffix_part)
);

-- Delete originals that were split
DELETE m
FROM sinir.mtr_load m
INNER JOIN tmp_mtr_split t
ON t.url = m.url;

DROP TEMPORARY TABLE IF EXISTS tmp_mtr_split;

COMMIT;
END$$
DELIMITER ;
