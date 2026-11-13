# Benchmark da outbox

## Medição real

Execução em PostgreSQL 16 e LocalStack 4.9.2 via Docker Compose, Node.js 24.12.0 no host, 200 eventos por versão, todos com carteiras independentes. O banco descartável estava sem backlog pendente. Uma única execução, sem warm-up e sem falhas injetadas; os percentis são descritivos desta amostra, não uma estimativa estatística estável.

| Métrica | Publisher original | Publisher otimizado |
|---|---:|---:|
| Eventos publicados | 200 | 200 |
| Vazão | 24,99 eventos/s | 512,34 eventos/s |
| Tempo para drenar | 8.002,01 ms | 390,37 ms |
| Latência da submissão SQS até confirmação SQL, p50 / p95 / p99 | 4,02 / 5,66 / 7,45 ms | 13,40 / 18,89 / 19,39 ms |
| Idade do evento até confirmação, p50 / p95 / p99 | 3.572 / 7.971 / 8.001 ms | 92 / 126 / 126 ms |
| Chamadas SQS | 200 | 20 |
| Confirmações SQL | 200 | 5 |
| Falhas SQS / retries / falhas SQL | 0 / 0 / 0 | 0 / 0 / 0 |
| Backlog antes → depois | 200 → 0; idade 0,006 s → 0 s | 200 → 0; idade 0,018 s → 0 s |

A vazão ficou 20,5 vezes maior e a drenagem levou 95,1% menos tempo nesta execução. As chamadas SQS caíram 90% e as confirmações SQL, 97,5%. A latência isolada de envio até confirmação subiu: cada evento agora aguarda o resultado do lote e a confirmação SQL conjunta. A idade total na fila caiu bastante porque o publisher não espera um segundo entre rodadas.

## Reprodução

Com Docker Compose, crie e migre um banco descartável e mantenha LocalStack disponível. Então execute no PowerShell:

```powershell
$env:ALLOW_DISPOSABLE_DB = 'YES'
$env:BENCH_DATABASE_URL = 'postgresql://wager:wager@localhost:5432/wagering_perf'
$env:SQS_ENDPOINT_URL = 'http://localhost:4566'
$env:SQS_REGION = 'us-east-1'
$env:SQS_ACCESS_KEY = 'test'
$env:SQS_SECRET_KEY = 'test'
npm.cmd run benchmark:outbox
```

Use `BENCH_EVENTS` para alterar a quantidade. O programa exige que não haja backlog pendente, cria duas filas FIFO temporárias, insere eventos de benchmark na outbox, mede ambos os caminhos contra PostgreSQL e LocalStack reais e remove as filas e os registros no `finally`. O script exige `ALLOW_DISPOSABLE_DB=YES` para reduzir o risco de apontar acidentalmente para uma base de dados importante.

Não houve injeção de falhas no benchmark. Falhas parciais SQS e erros no UPDATE de confirmação são cobertos por testes separados; parte deles usa adaptadores simulados e parte PostgreSQL/LocalStack reais.
