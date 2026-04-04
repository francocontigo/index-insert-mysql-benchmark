# index-insert-mysql-benchmark

Este projeto tem como objetivo analisar o impacto da quantidade e do tipo de índices na performance de inserções em uma tabela MySQL. O benchmark compara diferentes cenários de índices e tipos de dados (INT vs UUID) durante operações massivas de insert.

## Descrição

O script principal (`benchmark_indexes.py`) executa benchmarks de inserção concorrente em uma tabela MySQL, variando:

- Quantidade de índices (nenhum, poucos, médio, muitos)
- Tipo de dados das colunas (INT ou UUID)
- Volume de dados (ex: 50.000 e 200.000 linhas)
- Número de threads para inserção paralela

Os resultados podem ser analisados e visualizados no notebook `analysis_indexes.ipynb`, que utiliza pandas e matplotlib para gerar gráficos e sumarizar os dados coletados.

## Como funciona

1. O banco de dados é resetado a cada rodada de teste.
2. A tabela é criada com o tipo de dado e índices definidos pelo cenário.
3. Os dados são gerados aleatoriamente (INT ou UUID).
4. As inserções são feitas de forma concorrente usando múltiplas threads.
5. O tempo médio de inserção e throughput são calculados para cada cenário.

## Requisitos

- Python 3.8+
- MySQL Server (pode ser via Docker)
- Bibliotecas Python: `mysql-connector-python`, `pandas`, `matplotlib`


Instale as dependências com:

```bash
pip install -r requirements.txt
```

### Subindo o MySQL com Docker

Você pode subir um container MySQL 8.0 com o seguinte comando:

```bash
docker run --name mysql-benchmark -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=test -p 3306:3306 -d mysql:8.0
```

## Configuração

Edite o dicionário `CONFIG` no início do arquivo `benchmark_indexes.py` para ajustar as credenciais de acesso ao seu MySQL.

```python
CONFIG = {
	"host": "127.0.0.1",
	"user": "root",
	"password": "root",
	"database": "test"
}
```

## Execução

Para rodar o benchmark:

```bash
python benchmark_indexes.py
```

Os resultados serão impressos no terminal.

Para analisar e visualizar os resultados, utilize o notebook Jupyter `analysis_indexes.ipynb`.

## Estrutura

- `benchmark_indexes.py`: Script principal de benchmark.
- `analysis_indexes.ipynb`: Notebook para análise e visualização dos resultados.
- `README.md`: Este arquivo.

## Licença

MIT