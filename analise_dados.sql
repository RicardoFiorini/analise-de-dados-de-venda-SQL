-- Criação do banco de dados
CREATE DATABASE AnaliseDeDadosVendas;
USE AnaliseDeDadosVendas;

-- Tabela para armazenar informações de clientes
CREATE TABLE Clientes (
    cliente_id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    telefone VARCHAR(20),
    data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para armazenar informações de produtos
CREATE TABLE Produtos (
    produto_id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    categoria VARCHAR(50),
    preco DECIMAL(10, 2) NOT NULL,
    estoque INT NOT NULL CHECK (estoque >= 0),
    data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para armazenar informações de vendas
CREATE TABLE Vendas (
    venda_id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    produto_id INT NOT NULL,
    quantidade INT NOT NULL CHECK (quantidade > 0),
    data_venda DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cliente_id) REFERENCES Clientes(cliente_id) ON DELETE CASCADE,
    FOREIGN KEY (produto_id) REFERENCES Produtos(produto_id) ON DELETE CASCADE
);

-- Índices para melhorar a performance
CREATE INDEX idx_cliente_nome ON Clientes(nome);
CREATE INDEX idx_produto_nome ON Produtos(nome);
CREATE INDEX idx_venda_data ON Vendas(data_venda);
CREATE INDEX idx_venda_cliente ON Vendas(cliente_id);
CREATE INDEX idx_venda_produto ON Vendas(produto_id);

-- View para analisar vendas por cliente
CREATE VIEW ViewVendasPorCliente AS
SELECT c.cliente_id, c.nome AS cliente, COUNT(v.venda_id) AS total_vendas, SUM(v.quantidade) AS total_quantidade, SUM(v.quantidade * p.preco) AS total_gasto
FROM Clientes c
LEFT JOIN Vendas v ON c.cliente_id = v.cliente_id
LEFT JOIN Produtos p ON v.produto_id = p.produto_id
GROUP BY c.cliente_id
ORDER BY total_gasto DESC;

-- View para analisar vendas por produto
CREATE VIEW ViewVendasPorProduto AS
SELECT p.produto_id, p.nome AS produto, COUNT(v.venda_id) AS total_vendas, SUM(v.quantidade) AS total_vendido, SUM(v.quantidade * p.preco) AS total_faturado
FROM Produtos p
LEFT JOIN Vendas v ON p.produto_id = v.produto_id
GROUP BY p.produto_id
ORDER BY total_faturado DESC;

-- Função para calcular o faturamento total em um período específico
DELIMITER //
CREATE FUNCTION FaturamentoTotal(inicio DATETIME, fim DATETIME) RETURNS DECIMAL(10, 2)
BEGIN
    DECLARE total DECIMAL(10, 2);
    SELECT SUM(v.quantidade * p.preco) INTO total
    FROM Vendas v
    JOIN Produtos p ON v.produto_id = p.produto_id
    WHERE v.data_venda BETWEEN inicio AND fim;
    RETURN IFNULL(total, 0);
END //
DELIMITER ;

-- Trigger para atualizar o estoque após uma venda
DELIMITER //
CREATE TRIGGER Trigger_AntesInserirVenda
BEFORE INSERT ON Vendas
FOR EACH ROW
BEGIN
    DECLARE estoque_atual INT;
    SELECT estoque INTO estoque_atual FROM Produtos WHERE produto_id = NEW.produto_id;
    IF estoque_atual < NEW.quantidade THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Estoque insuficiente para a venda.';
    ELSE
        UPDATE Produtos SET estoque = estoque - NEW.quantidade WHERE produto_id = NEW.produto_id;
    END IF;
END //
DELIMITER ;

-- Inserção de exemplo de clientes
INSERT INTO Clientes (nome, email, telefone) VALUES 
('João Silva', 'joao.silva@example.com', '123456789'),
('Maria Oliveira', 'maria.oliveira@example.com', '987654321'),
('Pedro Santos', 'pedro.santos@example.com', '456123789');

-- Inserção de exemplo de produtos
INSERT INTO Produtos (nome, categoria, preco, estoque) VALUES 
('Produto A', 'Categoria 1', 100.00, 50),
('Produto B', 'Categoria 2', 150.00, 30),
('Produto C', 'Categoria 1', 200.00, 20);

-- Inserção de exemplo de vendas
INSERT INTO Vendas (cliente_id, produto_id, quantidade) VALUES 
(1, 1, 2),
(1, 2, 1),
(2, 1, 3),
(3, 2, 2),
(2, 3, 1);

-- Selecionar vendas por cliente
SELECT * FROM ViewVendasPorCliente;

-- Selecionar vendas por produto
SELECT * FROM ViewVendasPorProduto;

-- Calcular faturamento total em um período específico
SELECT FaturamentoTotal('2024-10-01', '2024-10-31') AS faturamento_outubro;

-- Excluir uma venda (isso atualiza o estoque automaticamente)
DELETE FROM Vendas WHERE venda_id = 1;

-- Excluir um produto (isso falhará se o produto tiver vendas)
DELETE FROM Produtos WHERE produto_id = 1;

-- Excluir um cliente (isso falhará se o cliente tiver vendas)
DELETE FROM Clientes WHERE cliente_id = 1;
