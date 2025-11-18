-- 1. Configurações
CREATE DATABASE IF NOT EXISTS AnaliseDeDadosVendas
CHARACTER SET utf8mb4
COLLATE utf8mb4_0900_ai_ci;

USE AnaliseDeDadosVendas;

-- =========================================================
-- 📦 DIMENSÕES (Quem? Onde? O Que? Quando?)
-- =========================================================

-- DIMENSÃO TEMPO (Essencial para BI e Relatórios Rápidos)
-- Permite queries como "Vendas em Finais de Semana" sem processamento pesado
CREATE TABLE DimTempo (
    data_id DATE PRIMARY KEY,
    dia INT,
    mes INT,
    ano INT,
    trimestre INT,
    dia_semana VARCHAR(20),
    eh_fim_de_semana BOOLEAN,
    nome_mes VARCHAR(20)
);

-- DIMENSÃO CLIENTE (Com Segmentação RFM)
CREATE TABLE Clientes (
    cliente_id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    cidade VARCHAR(50),
    estado CHAR(2),
    
    -- Métricas Analíticas (Atualizadas via Procedure)
    segmento_rfm ENUM('Novo', 'Promissor', 'Campeão', 'Em Risco', 'Perdido') DEFAULT 'Novo',
    score_fidelidade DECIMAL(5,2) DEFAULT 0,
    
    data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_segmento (segmento_rfm)
);

-- DIMENSÃO PRODUTO
CREATE TABLE Produtos (
    produto_id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    categoria VARCHAR(50),
    sku VARCHAR(50) UNIQUE,
    custo_atual DECIMAL(10, 2), -- Para cálculo de margem
    preco_atual DECIMAL(10, 2) NOT NULL,
    estoque_atual INT NOT NULL DEFAULT 0,
    ativo BOOLEAN DEFAULT TRUE
);

-- =========================================================
-- 📈 FATOS E TRANSAÇÕES (O Acontecimento)
-- =========================================================

-- CABEÇALHO DO PEDIDO (Fato Venda)
CREATE TABLE Pedidos (
    pedido_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    data_pedido DATETIME DEFAULT CURRENT_TIMESTAMP,
    data_id DATE GENERATED ALWAYS AS (DATE(data_pedido)) STORED, -- Link para DimTempo
    
    valor_total DECIMAL(12, 2) DEFAULT 0.00,
    status ENUM('Pendente', 'Pago', 'Cancelado') DEFAULT 'Pendente',
    
    FOREIGN KEY (cliente_id) REFERENCES Clientes(cliente_id),
    FOREIGN KEY (data_id) REFERENCES DimTempo(data_id),
    
    INDEX idx_data_status (data_pedido, status) -- O índice mais usado em dashboards
);

-- DETALHE DO PEDIDO (Onde mora a verdade histórica)
CREATE TABLE ItensPedido (
    item_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pedido_id BIGINT NOT NULL,
    produto_id INT NOT NULL,
    
    quantidade INT NOT NULL,
    
    -- SNAPSHOTS (Preço congelado no momento da venda)
    preco_unitario_venda DECIMAL(10, 2) NOT NULL,
    custo_unitario_momento DECIMAL(10, 2) NOT NULL, -- Para saber o lucro real daquele dia
    
    subtotal DECIMAL(12, 2) GENERATED ALWAYS AS (quantidade * preco_unitario_venda) STORED,
    margem_item DECIMAL(12, 2) GENERATED ALWAYS AS (subtotal - (quantidade * custo_unitario_momento)) STORED,
    
    FOREIGN KEY (pedido_id) REFERENCES Pedidos(pedido_id) ON DELETE CASCADE,
    FOREIGN KEY (produto_id) REFERENCES Produtos(produto_id)
);

-- =========================================================
-- 🧠 PROCEDURES E INTEGRIDADE ANALÍTICA
-- =========================================================

-- PROCEDURE: Popular Dimensão Tempo (Executar uma vez para os próximos 10 anos)
DELIMITER //
CREATE PROCEDURE sp_PopularDimTempo(IN p_ano_inicio INT, IN p_ano_fim INT)
BEGIN
    DECLARE v_data DATE;
    SET v_data = DATE(CONCAT(p_ano_inicio, '-01-01'));
    
    WHILE YEAR(v_data) <= p_ano_fim DO
        INSERT IGNORE INTO DimTempo (data_id, dia, mes, ano, trimestre, dia_semana, eh_fim_de_semana, nome_mes)
        VALUES (
            v_data, 
            DAY(v_data), 
            MONTH(v_data), 
            YEAR(v_data), 
            QUARTER(v_data), 
            DAYNAME(v_data),
            CASE WHEN DAYOFWEEK(v_data) IN (1, 7) THEN TRUE ELSE FALSE END,
            MONTHNAME(v_data)
        );
        SET v_data = DATE_ADD(v_data, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;

-- TRIGGER: Congelar Preço e Atualizar Estoque
DELIMITER //
CREATE TRIGGER trg_AntesInserirItem
BEFORE INSERT ON ItensPedido
FOR EACH ROW
BEGIN
    DECLARE v_preco DECIMAL(10,2);
    DECLARE v_custo DECIMAL(10,2);
    DECLARE v_estoque INT;
    
    -- 1. Busca dados atuais do produto
    SELECT preco_atual, custo_atual, estoque_atual 
    INTO v_preco, v_custo, v_estoque
    FROM Produtos WHERE produto_id = NEW.produto_id;
    
    -- 2. Validação de Estoque
    IF v_estoque < NEW.quantidade THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Erro: Estoque insuficiente.';
    END IF;
    
    -- 3. O "Snapshot" (Congelamento de valores)
    SET NEW.preco_unitario_venda = v_preco;
    SET NEW.custo_unitario_momento = v_custo;
    
    -- 4. Baixa no Estoque
    UPDATE Produtos SET estoque_atual = estoque_atual - NEW.quantidade 
    WHERE produto_id = NEW.produto_id;
END //
DELIMITER ;

-- TRIGGER: Atualizar Total do Pedido
DELIMITER //
CREATE TRIGGER trg_AposInserirItem
AFTER INSERT ON ItensPedido
FOR EACH ROW
BEGIN
    UPDATE Pedidos 
    SET valor_total = valor_total + NEW.subtotal 
    WHERE pedido_id = NEW.pedido_id;
END //
DELIMITER ;

-- PROCEDURE AVANÇADA: Análise RFM (Recency, Frequency, Monetary)
-- Classifica clientes automaticamente baseado no comportamento de compra
DELIMITER //
CREATE PROCEDURE sp_CalcularRFM()
BEGIN
    -- Tabela temporária para scoring
    CREATE TEMPORARY TABLE TempRFM AS
    SELECT 
        p.cliente_id,
        DATEDIFF(NOW(), MAX(p.data_pedido)) AS recencia_dias, -- Há quanto tempo não compra
        COUNT(DISTINCT p.pedido_id) AS frequencia_total, -- Quantas vezes comprou
        SUM(p.valor_total) AS valor_monetario -- Quanto gastou
    FROM Pedidos p
    WHERE p.status = 'Pago'
    GROUP BY p.cliente_id;

    -- Atualiza tabela de clientes com segmentação
    UPDATE Clientes c
    JOIN TempRFM r ON c.cliente_id = r.cliente_id
    SET c.segmento_rfm = CASE
        WHEN r.valor_monetario > 5000 AND r.frequencia_total > 10 THEN 'Campeão'
        WHEN r.recencia_dias > 90 AND r.valor_monetario > 1000 THEN 'Em Risco'
        WHEN r.recencia_dias < 30 AND r.frequencia_total = 1 THEN 'Novo'
        WHEN r.valor_monetario > 2000 THEN 'Promissor'
        ELSE 'Promissor' -- Simplificação para o exemplo
    END;
    
    DROP TEMPORARY TABLE TempRFM;
END //
DELIMITER ;

-- VIEW: Dashboard Executivo (Lucro Real)
CREATE OR REPLACE VIEW v_DashboardLucratividade AS
SELECT 
    t.ano,
    t.nome_mes,
    cat.categoria,
    SUM(i.subtotal) AS faturamento_bruto,
    SUM(i.margem_item) AS lucro_liquido,
    CONCAT(ROUND((SUM(i.margem_item) / SUM(i.subtotal)) * 100, 1), '%') AS margem_percentual
FROM ItensPedido i
JOIN Pedidos p ON i.pedido_id = p.pedido_id
JOIN DimTempo t ON p.data_id = t.data_id
JOIN Produtos cat ON i.produto_id = cat.produto_id
WHERE p.status = 'Pago'
GROUP BY t.ano, t.mes, t.nome_mes, cat.categoria
ORDER BY t.ano DESC, t.mes DESC;
