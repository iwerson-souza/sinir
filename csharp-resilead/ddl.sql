CREATE DATABASE IF NOT EXISTS resilead CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE resilead;

-- ==========================================================
-- Tabelas de referência
-- ==========================================================

CREATE TABLE situacao (
  id_situacao INT AUTO_INCREMENT PRIMARY KEY,
  descricao VARCHAR(80) NOT NULL UNIQUE
) ENGINE=InnoDB;

CREATE TABLE tipo_manifesto (
  id_tipo_manifesto INT AUTO_INCREMENT PRIMARY KEY,
  descricao VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB;

CREATE TABLE tratamento (
  id_tratamento INT AUTO_INCREMENT PRIMARY KEY,
  descricao VARCHAR(120) NOT NULL UNIQUE
) ENGINE=InnoDB;

CREATE TABLE unidade (
  codigo_unidade INT PRIMARY KEY,
  descricao VARCHAR(50) NOT NULL,
  sigla VARCHAR(10) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE classe (
  codigo_classe INT PRIMARY KEY,
  descricao VARCHAR(50) NOT NULL,
  resolucao VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- ==========================================================
-- Entidades e relacionamentos
-- ==========================================================

CREATE TABLE entidade (
  id_entidade BIGINT AUTO_INCREMENT PRIMARY KEY,
  cpf_cnpj VARCHAR(20) NOT NULL UNIQUE,
  cpf_cnpj_hash CHAR(64),
  nome_razao_social VARCHAR(500) NOT NULL,
  nome_fantasia VARCHAR(500) NULL,
  tipo_pessoa ENUM('F','J') NOT NULL,
  
  uf CHAR(2),
  municipio VARCHAR(100),
  codigo_municipio_ibge INT,
  cep VARCHAR(10),
  logradouro VARCHAR(255),
  numero VARCHAR(100),
  complemento VARCHAR(100),
  bairro VARCHAR(120),
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6),
  
  porte VARCHAR(50),
  data_inicio_atividade DATE,
  cnae_principal VARCHAR(20),
  cnae_principal_descricao VARCHAR(255),
  
  data_criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  data_atualizacao DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE tipo_entidade (
  id_tipo_entidade BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_entidade BIGINT NOT NULL,
  tipo ENUM('GERADOR','TRANSPORTADOR','DESTINADOR') NOT NULL,
  CONSTRAINT fk_tipo_entidade_entidade FOREIGN KEY (id_entidade) REFERENCES entidade(id_entidade)
) ENGINE=InnoDB;

CREATE TABLE entidade_motorista (
  id_motorista BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_entidade BIGINT NOT NULL,
  nome VARCHAR(255),
  proprio BOOLEAN,
  CONSTRAINT fk_motorista_entidade FOREIGN KEY (id_entidade) REFERENCES entidade(id_entidade)
) ENGINE=InnoDB;

CREATE TABLE entidade_veiculo (
  id_veiculo BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_entidade BIGINT NOT NULL,
  placa_veiculo VARCHAR(10) NOT NULL,
  CONSTRAINT fk_veiculo_entidade FOREIGN KEY (id_entidade) REFERENCES entidade(id_entidade)
) ENGINE=InnoDB;

CREATE TABLE entidade_responsavel (
  id_responsavel BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_entidade BIGINT NOT NULL,
  nome VARCHAR(255),
  tipo_responsavel ENUM('EMISSAO','RECEBIMENTO') NOT NULL,
  CONSTRAINT fk_responsavel_entidade FOREIGN KEY (id_entidade) REFERENCES entidade(id_entidade)
) ENGINE=InnoDB;

CREATE TABLE `entidade_unidade` (
  `id_unidade` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_entidade` bigint(20) NOT NULL,
  `unidade` varchar(32) NOT NULL,
  `endereco` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_unidade`),
  KEY `fk_unidade_entidade` (`id_entidade`),
  CONSTRAINT `fk_unidade_entidade` FOREIGN KEY (`id_entidade`) REFERENCES `entidade` (`id_entidade`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ==========================================================
-- Catálogo de resíduos
-- ==========================================================

CREATE TABLE residuo (
  codigo_residuo VARCHAR(20) PRIMARY KEY,
  descricao VARCHAR(500) NOT NULL,
  perigoso BOOLEAN,
  codigo_unidade_padrao INT,
  CONSTRAINT fk_residuo_unidade FOREIGN KEY (codigo_unidade_padrao) REFERENCES unidade(codigo_unidade)
) ENGINE=InnoDB;

-- ==========================================================
-- Registro principal do MTR
-- ==========================================================

CREATE TABLE registro (
  id_registro BIGINT AUTO_INCREMENT PRIMARY KEY,
  numero_mtr VARCHAR(20) NOT NULL UNIQUE,
  id_tipo_manifesto INT,
  id_gerador BIGINT,
  id_transportador BIGINT,
  id_destinador BIGINT,
  id_entidade_resp_emissao BIGINT,
  id_entidade_resp_recebimento BIGINT,
  id_situacao INT,
  id_tratamento INT,
  numero_cdf VARCHAR(50),
  justificativa TEXT,
  data_emissao DATE,
  data_recebimento DATE,
  data_criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  data_atualizacao DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_mtr_tipo_manifesto FOREIGN KEY (id_tipo_manifesto) REFERENCES tipo_manifesto(id_tipo_manifesto),
  CONSTRAINT fk_mtr_gerador FOREIGN KEY (id_gerador) REFERENCES entidade(id_entidade),
  CONSTRAINT fk_mtr_transportador FOREIGN KEY (id_transportador) REFERENCES entidade(id_entidade),
  CONSTRAINT fk_mtr_destinador FOREIGN KEY (id_destinador) REFERENCES entidade(id_entidade),
  CONSTRAINT fk_mtr_resp_emissao FOREIGN KEY (id_entidade_resp_emissao) REFERENCES entidade_responsavel(id_responsavel),
  CONSTRAINT fk_mtr_resp_recebimento FOREIGN KEY (id_entidade_resp_recebimento) REFERENCES entidade_responsavel(id_responsavel),
  CONSTRAINT fk_mtr_situacao FOREIGN KEY (id_situacao) REFERENCES situacao(id_situacao),
  CONSTRAINT fk_mtr_tratamento FOREIGN KEY (id_tratamento) REFERENCES tratamento(id_tratamento)
) ENGINE=InnoDB;

-- ==========================================================
-- Relação de resíduos dentro do MTR
-- ==========================================================

CREATE TABLE registro_residuo (
  id_registro_residuo BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_registro BIGINT NOT NULL,
  codigo_residuo VARCHAR(20),
  codigo_classe INT,
  codigo_unidade INT,  
  quantidade_indicada DECIMAL(14,4),
  quantidade_recebida DECIMAL(14,4),
  data_criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  data_atualizacao DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  
  CONSTRAINT fk_reg_residuo_registro FOREIGN KEY (id_registro) REFERENCES registro(id_registro),
  CONSTRAINT fk_reg_residuo_residuo FOREIGN KEY (codigo_residuo) REFERENCES residuo(codigo_residuo),
  CONSTRAINT fk_reg_residuo_classe FOREIGN KEY (codigo_classe) REFERENCES classe(codigo_classe),
  CONSTRAINT fk_reg_residuo_unidade FOREIGN KEY (codigo_unidade) REFERENCES unidade(codigo_unidade)  
) ENGINE=InnoDB;

-- ==========================================================
-- Índices auxiliares
-- ==========================================================

CREATE INDEX idx_entidade_nome_razao_social ON entidade (nome_razao_social);
CREATE INDEX idx_entidade_nome_fantasia ON entidade (nome_fantasia);
CREATE INDEX idx_entidade_cpf_cnpj ON entidade (cpf_cnpj);
CREATE INDEX idx_entidade_uf ON entidade (uf);
CREATE INDEX idx_entidade_municipio_ibge ON entidade (codigo_municipio_ibge);

/*
  id_tipo_manifesto INT,

  id_situacao INT,
  id_tratamento INT,
*/
CREATE INDEX idx_reg_gerador ON registro (id_gerador);
CREATE INDEX idx_reg_transportador ON registro (id_transportador);
CREATE INDEX idx_reg_destinador ON registro (id_destinador);

CREATE INDEX idx_reg_data_emissao ON registro (data_emissao);
CREATE INDEX idx_reg_situacao ON registro (id_situacao);
CREATE INDEX idx_reg_tratamento ON registro (id_tratamento);

CREATE INDEX idx_reg_residuo ON registro_residuo (id_registro);
CREATE INDEX idx_reg_residuo_codigo ON registro_residuo (codigo_residuo);
