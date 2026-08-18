# Runbook

## Setup no Databricks Free Edition

1. Crie a conta em <https://www.databricks.com/learn/free-edition> (não pede cartão).
2. No workspace, abra um notebook e rode `notebooks/00_setup.py` — ele cria o
   catálogo `nyc_taxi`, os três schemas e o Volume `bronze.landing`.
3. **Baixe os parquets na sua máquina** e suba para o Volume:

   ```bash
   python scripts/download_data.py --months 2024-01 2024-02 2024-03
   ```

   Depois, no workspace: `Catalog → nyc_taxi → bronze → landing → Upload`.

   > Por que manual? O Free Edition restringe a saída para a internet a domínios
   > confiáveis, e o CloudFront da TLC não está na lista. Em workspaces pagos com
   > internet liberada, troque `source.mode` para `http` e o pipeline baixa sozinho.

4. Conecte o repositório em `Workspace → Repos → Add Repo` e rode os notebooks
   `01` → `02` → `03`, ou publique o job:

   ```bash
   databricks bundle deploy -t dev
   databricks bundle run nyc_taxi_pipeline -t dev
   ```

## Setup local

```bash
make install
python scripts/download_data.py --months 2024-01
make run
```

O lakehouse local é gravado em `./lakehouse`. `make clean` apaga tudo.

## Operações comuns

### Reprocessar um mês específico

```bash
python -m nyc_taxi.run --layer all --months 2024-02
```

Idempotente: substitui só as partições de `2024-02`.

### Rodar só a partir do silver (depois de mudar uma regra)

```bash
python -m nyc_taxi.run --layer silver --months 2024-01 2024-02 2024-03
python -m nyc_taxi.run --layer gold   --months 2024-01 2024-02 2024-03
```

Bronze não precisa ser tocado — é justamente para isso que ele existe.

### Investigar uma falha de qualidade

```sql
-- o que falhou e quando
SELECT * FROM gold.dq_results WHERE NOT passed ORDER BY run_at DESC;

-- quais regras estão rejeitando mais linhas
SELECT motivo, count(*) AS linhas
FROM silver.fct_source_trips_quarantine
LATERAL VIEW explode(_violations) v AS motivo
GROUP BY motivo ORDER BY linhas DESC;

-- amostra das linhas rejeitadas por uma regra
SELECT * FROM silver.fct_source_trips_quarantine
WHERE array_contains(_violations, 'distance_positive') LIMIT 20;
```

### Voltar no tempo (Delta time travel)

```sql
DESCRIBE HISTORY gold.fct_trips;
SELECT count(*) FROM gold.fct_trips VERSION AS OF 3;
RESTORE TABLE gold.fct_trips TO VERSION AS OF 3;
```

## Problemas conhecidos

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Unable to tunnel through proxy` ao subir o Spark local | Maven bloqueado na rede | Baixe o jar `delta-spark_2.12` manualmente e passe via `--jars` |
| `Path does not exist: /Volumes/...` | Parquets não enviados ao Volume | Rode o passo 3 do setup |
| Compute derruba no meio do job | Quota diária do Free Edition | Reduza `ingestion.months` para 1 mês |
| `fact_matches_silver_rowcount` falha | Gold rodou com `--months` diferente do silver | Rode as duas camadas com a mesma lista de meses |
| Muitos arquivos pequenos no Delta | `optimizeWrite` desligado | `OPTIMIZE <tabela> ZORDER BY (...)` |

## Evolução planejada

Em ordem de custo/benefício para quem quiser estender o projeto:

1. **Auto Loader no bronze** — troca a leitura de path por
   `readStream.format("cloudFiles")`, com checkpoint. Ingestão incremental real,
   sem reprocessar o mês inteiro.
2. **Change Data Feed no silver + `MERGE` na fato** — hoje a gold reescreve
   partições; com CDF, só as linhas alteradas chegam à fato.
3. **Lakeflow Declarative Pipelines (ex-DLT)** — as expectativas do YAML viram
   `@dlt.expect_or_drop`, com linhagem e métricas nativas na UI.
4. **Dashboard no Databricks SQL** — as queries de `scripts/analytics_queries.sql`
   já estão prontas para virar visualizações.
5. **dbt sobre a camada gold** — se o time preferir SQL a PySpark na modelagem.
