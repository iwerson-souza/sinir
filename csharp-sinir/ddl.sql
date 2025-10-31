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
  status        ENUM('PENDING','PROCESSING','DONE','ERROR') NOT NULL DEFAULT 'PENDING',
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

