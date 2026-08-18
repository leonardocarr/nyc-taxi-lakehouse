# Roadmap de desenvolvimento

Plano para levar o projeto de zero a um repositório que sustenta uma conversa
técnica em entrevista. As fases 1–4 estão **entregues neste repositório**; as
demais são o caminho de evolução.

O tempo estimado assume algumas horas por dia, não dedicação integral.

---

## Fase 0 — Ambiente (½ dia) ✅

- Conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition) — não pede cartão.
- Repositório no GitHub, ambiente local com `make install`.
- Rodar `notebooks/00_setup.py` e subir 1 mês de dados para o Volume.

**Critério de pronto:** `make test` passa na sua máquina e o catálogo `nyc_taxi`
existe no workspace.

---

## Fase 1 — Bronze (1 dia) ✅

Ingestão crua com colunas de linhagem, particionada por `load_month`, com escrita
idempotente via `replaceWhere`.

**Armadilha comum:** aplicar filtro ou cast já no bronze. Se você fizer isso, perde
a capacidade de reprocessar quando a regra mudar — e ela vai mudar.

**Critério de pronto:** rodar a ingestão de `2024-01` três vezes seguidas produz
exatamente a mesma contagem.

---

## Fase 2 — Silver (2 dias) ✅

Normalização de schema, casts explícitos, `trip_id` determinístico, deduplicação,
regras de validação em YAML e tabela de quarentena.

**O detalhe que diferencia:** quarentena em vez de `WHERE ... > 0`. Quando alguém
perguntar por que o total não bate com o relatório oficial da TLC, você tem a
resposta em uma query.

**Critério de pronto:** `bronze = silver + quarentena` para todos os meses, e a
distribuição de motivos de rejeição faz sentido de negócio.

---

## Fase 3 — Gold (2 dias) ✅

Star schema: `fct_trips` + `dim_date`, `dim_time`, `dim_location` (SCD2),
`dim_vendor`, `dim_rate_code`, `dim_payment_type`. Membro "Não informado" em toda
dimensão.

**A parte que quase todo mundo erra:** o join da fato com a dimensão SCD2 usando a
versão vigente na data do evento, não `is_current`. Sem isso, o SCD2 é decorativo.

**Critério de pronto:** as 8 queries de `scripts/analytics_queries.sql` rodam e
respondem perguntas de negócio sem CTE aninhada.

---

## Fase 4 — Qualidade, testes e orquestração (2 dias) ✅

Motor de expectativas com histórico em `gold.dq_results`, 27 testes com PySpark +
Delta reais, Asset Bundle com job de 3 tasks, CI no GitHub Actions.

**Por que isso pesa mais que uma camada extra:** um repositório com testes e CI
verde comunica "sei entregar em produção". Um com cinco camadas e nenhum teste
comunica o contrário.

**Critério de pronto:** badge de CI verde no README e `databricks bundle run`
executando o pipeline ponta a ponta.

---

## Fase 5 — Ingestão incremental (2 dias)

Trocar a leitura de path por Auto Loader:

```python
(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
      .load(landing_path)
      .writeStream
      .option("checkpointLocation", f"{checkpoint}/bronze")
      .trigger(availableNow=True)
      .toTable(bronze_table))
```

Depois: Change Data Feed no silver (`delta.enableChangeDataFeed = true`) e `MERGE`
incremental na fato, em vez de reescrever partições.

**Ganho para o portfólio:** mostra que você entende a diferença entre "recarregar
tudo" e "processar o delta", que é a pergunta padrão em entrevista.

---

## Fase 6 — Camada de consumo (1 dia)

Dashboard no Databricks SQL a partir de `scripts/analytics_queries.sql`, ou um
`.pbix` conectado via SQL Warehouse. Screenshot no README.

**Por que importa:** o recrutador que abre o repositório não vai rodar seu código.
Ele vai olhar o README. Uma imagem do resultado final muda a primeira impressão.

---

## Fase 7 — Opcionais, por ordem de retorno

| Item | Retorno | Esforço |
|---|---|---|
| Lakeflow Declarative Pipelines (ex-DLT) | Alto — é o padrão que a Databricks empurra | 2 dias |
| dbt sobre a gold | Alto se a vaga citar dbt | 2 dias |
| Segundo dataset (clima) para enriquecer a fato | Médio — mostra join entre fontes | 1 dia |
| Terraform para o workspace | Baixo para vaga de DE júnior/pleno | 2 dias |
| Great Expectations no lugar do motor próprio | Baixo — o motor atual já demonstra o conceito | 1 dia |

---

## O que colocar no currículo depois

> Pipeline ELT em Databricks (PySpark + Delta Lake) sobre ~9M de corridas de táxi
> da NYC, com arquitetura medalhão, modelagem dimensional (1 fato + 6 dimensões,
> incluindo SCD Tipo 2), validação de qualidade com quarentena de rejeitos,
> cargas idempotentes e orquestração via Databricks Asset Bundles com CI.

Números concretos > lista de tecnologias. Depois de rodar 3 meses de dados, meça
e substitua: volume ingerido, tempo de execução por camada, taxa de rejeição.

## Perguntas que este projeto te prepara para responder

1. *Por que medalhão e não só duas camadas?* → Contrato por camada em `docs/architecture.md`.
2. *Como você garante idempotência?* → `replaceWhere` + `trip_id` determinístico.
3. *O que acontece se chegar dado sujo?* → Quarentena, não descarte.
4. *Por que SCD Tipo 2 aqui?* → Renomeação de zona não pode reescrever o passado.
5. *Como você testa um pipeline Spark?* → Funções puras + Spark local com Delta.
6. *Como você sabe que a carga de ontem estava correta?* → `gold.dq_results`.

Se você conseguir responder essas seis olhando o próprio código, o projeto cumpriu
o papel.
