# VPS дээр байршуулж бүрэн шалгах заавар

Энэ заавар нь төслийг цэвэр Linux VPS (Ubuntu 22.04/24.04) дээр эхнээс нь асааж, бүх
урсгалыг жинхэнэ орчинд шалгах дараалал юм. Алхам бүр **юу хийх**, **яагаад**, **амжилттай
болсныг яаж мэдэх** гэсэн гурван хэсэгтэй.

```text
0. Код GitHub руу      → 1. VPS шалгах   → 2. Docker      → 3. Clone + .env
4. Өгөгдлийн сангууд   → 5. Airflow image → 6. Source seed → 7. Airflow асаах
8. Pipeline ажиллуулах → 9. Үр дүн шалгах → 10. Dashboard  → 11. Цаг тутмын урсгал
12. Alert шалгах       → 13. OpenMetadata (8 GB+ RAM бол)
```

Бүх үйлчилгээ зөвхөн `127.0.0.1` дээр нээгддэг (интернэтэд ил гардаггүй). Браузераас
SSH tunnel-ээр орно (алхам 10).

---

## 0. Код GitHub руу (өөрийн компьютер дээр, PowerShell)

`ci.yml`-ийг `.github/workflows/ci.yml` замд хуулсан эсэхээ шалгаад:

```powershell
cd C:\Users\baasandorj\Private\DAMA-Mongolia-Capstone-Project
git checkout -b feature/capstone-improvements
git add -A
git status          # .env жагсаалтад ГАРАХ ЁСГҮЙ (.gitignore-д бий)
git commit -m "Capstone improvements: medallion, governance, monitoring, hourly generator"
git push -u origin feature/capstone-improvements
```

**Амжилт:** GitHub дээр branch харагдана, Actions таб дээр CI эхэлнэ.

---

## 1. VPS-ийг шалгах

```bash
ssh <user>@<vps-ip>
command -v git >/dev/null || (sudo apt-get update && sudo apt-get install -y git)
git clone https://github.com/Bskaa/DAMA-Mongolia-Capstone-Project.git ~/capstone
cd ~/capstone && git checkout feature/capstone-improvements
bash scripts/vps_preflight.sh
```

Repo private бол clone хийхэд GitHub-ийн Personal Access Token (нууц үгийн оронд) хэрэгтэй.

**Амжилт:** RAM, диск, Docker, портууд, сүлжээ бүгд `OK`. `FAIL` гарвал дараагийн алхмаар засна.

| Үр дүн | Шийдэл |
| --- | --- |
| RAM < 8 GB | OpenMetadata-г алгасна (алхам 13); swap нэмнэ (доор) |
| Docker байхгүй / daemon хүрэхгүй | Алхам 2 |
| Порт эзэлсэн (жишээ нь хичээлийн Airflow 8080 дээр) | `.env`-д `AIRFLOW_PORT=8081` гэх мэтээр өөрчилнө |

Swap нэмэх (RAM 8 GB-аас бага бол):

```bash
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

---

## 2. Docker суулгах (байхгүй бол)

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER" && newgrp docker
docker compose version
```

**Амжилт:** `docker compose version` v2.x хэвлэнэ; `docker run --rm hello-world` ажиллана.

---

## 3. `.env` үүсгэх

```bash
cd ~/capstone
python3 scripts/generate_env.py        # бүх нууц үгийг санамсаргүйгээр үүсгэнэ, хэвлэхгүй
nano .env                               # заавал биш: RAWG_API_KEY, SMTP, ALERT_NAME
python3 -m scripts.check_env            # 0 error гарах ёстой
```

**Яагаад:** placeholder нууц үг, давхардсан нууц үг, URL-д таарахгүй тэмдэгт зэрэг алдааг асаахаас
өмнө барина. `check_env` утгыг хэзээ ч хэвлэдэггүй.

---

## 4. Өгөгдлийн сангуудыг асаах

```bash
docker compose up -d --wait postgres-source postgres-analytics
docker compose ps
set -a; . ./.env; set +a                # .env-ийн хувьсагчдыг shell-д ачаална
docker compose exec -T postgres-source psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\dt'
docker compose exec -T postgres-analytics psql -U "$POSTGRES_USER" -d "$ANALYTICS_DB" -c '\du'
```

**Юу болдог:** шинэ volume дээр init script-үүд автоматаар ажиллана: source-д 8 хүснэгт үүснэ,
analytics-д `analytics_reader` ба `dashboard_user` role үүснэ.

**Амжилт:** хоёр service `healthy`; `\dt` дээр players, games, sessions… 8 хүснэгт; `\du` дээр
`dashboard_user`.

---

## 5. Airflow image бүтээх

```bash
docker compose --profile orchestration build airflow
```

**Юу болдог:** Airflow 3 image дээр төслийн virtualenv (dbt, psycopg, Faker, openpyxl...) болон
Meltano plugin-ууд суулгана. Анх 5–10 минут.

Дараагийн алхмуудад төслийн кодыг энэ image дотор ажиллуулах туслах функцууд:

```bash
run_project() {
  docker compose --profile orchestration run --rm --no-deps \
    -e PYTHONPATH=/opt/project \
    -e DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres-source:5432/${POSTGRES_DB}" \
    -e ANALYTICS_HOST=postgres-analytics -e ANALYTICS_PORT=5432 -e DBT_HOST=postgres-analytics \
    --workdir /opt/project --entrypoint /opt/project-venv/bin/python airflow "$@"
}
run_dbt() {
  # dbt artifacts go to /tmp inside the container (the airflow user cannot write
  # into your checkout); step 13 shows how to keep them for OpenMetadata.
  docker compose --profile orchestration run --rm --no-deps \
    -e ANALYTICS_PORT=5432 -e DBT_HOST=postgres-analytics \
    -e DBT_TARGET_PATH="${DBT_TARGET_PATH:-/tmp/dbt-target}" -e DBT_LOG_PATH=/tmp/dbt-logs \
    -e DBT_PROJECT_DIR=/opt/project/dbt -e DBT_PROFILES_DIR=/opt/project/dbt \
    --workdir /opt/project/dbt --entrypoint /opt/project-venv/bin/dbt airflow "$@"
}
```

**Амжилт:** `set -a; . ./.env; set +a` хийсний дараа `run_dbt debug` → `All checks passed!`

---

## 6. Source-ийг анхны өгөгдлөөр дүүргэх (нэг удаа)

Airflow асаахаас **өмнө** хийнэ: цаг тутмын generator DAG хоосон source дээр ажиллах гэж
оролдохоос сэргийлнэ.

```bash
run_project -m ingestion.generate.generate_gaming_data
```

**Юу болдог:** 10 000 тоглогч, 1 000 тоглоом, 100 000 session... behavior загвараар
(churn, хит тоглоом, campaign uplift, орон нутгийн валют). 2–5 минут.

**Амжилт:** JSON тайланд `"invalid_sessions": 0, "invalid_session_dates": 0`.

---

## 7. Airflow асаах

```bash
docker compose --profile orchestration up -d --wait airflow
docker compose exec airflow airflow dags list-import-errors     # "No data found" байх ёстой
docker compose exec airflow airflow dags list | grep -E 'gaming|pipeline|dbt|alert'
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated   # Airflow 3: {"admin": "..."}
```

**Юу болдог:** `postgres-airflow` metadata DB, scheduler, API/UI асна. 5 DAG харагдана:
`gaming_analytics_pipeline`, `gaming_source_data_generator`, `pipeline_watchdog`,
`alert_drill`, `dbt_weekly_full_refresh`.

DAG-ууд pause-гүй үүсдэг (`catchup=False`), тиймээс Airflow асмагц:
- `gaming_analytics_pipeline` сүүлийн өдрийн интервалаар **өөрөө нэг удаа эхэлж** магадгүй;
- `gaming_source_data_generator` тухайн цагийн batch-ийг нэмнэ;
- `pipeline_watchdog` эхлээд `pipeline_silent` гэж мэдээлж болно (pipeline дуусаагүй байхад) — хэвийн;
- `dbt_weekly_full_refresh` **pause-тай** үүснэ (өдрийн dbt-тэй зэрэг ажиллахаас сэргийлж). Pipeline
  амжилттай болсны дараа UI-аас эсвэл `airflow dags unpause dbt_weekly_full_refresh`-ээр асаана.

**Амжилт:** import error байхгүй; 5 DAG жагсаалтад байна. Нууц үг: Airflow 3-ийн simple auth manager
`simple_auth_manager_passwords.json.generated`-д `{"admin": "..."}` хэлбэрээр хадгална (файл
олдохгүй бол `docker compose logs airflow | grep -i password`).

---

## 8. Өдрийн pipeline-ийг ажиллуулах

Алхам 7-ийн дараа run өөрөө эхэлсэн эсэхийг харна (доорх query-д `gaming_analytics_pipeline`-ийн
task-ууд гарч байвал эхэлсэн). Эхлээгүй бол гараар:

```bash
docker compose exec airflow airflow dags trigger gaming_analytics_pipeline
```

Явцыг UI-аас (алхам 10-ын tunnel) эсвэл төслийн өөрийн бүртгэлээс харна:

```bash
docker compose exec -T postgres-analytics psql -U "$POSTGRES_USER" -d "$ANALYTICS_DB" -c "
  select task_id, try_number, state, duration_seconds, ended_at
  from monitoring.task_runs where dag_id = 'gaming_analytics_pipeline'
  order by ended_at desc limit 15"
```

Хүлээгдэх дараалал ба хугацаа (анхны ажиллагаа):

| Task | Юу хийдэг | Хугацаа |
| --- | --- | --- |
| check_databases | хоёр DB-д холбогдож чадах эсэх | секунд |
| sync_source_to_bronze | Meltano: 8 хүснэгтийг Bronze руу | 3–10 мин (анх бүтэн) |
| ingest_rawg | RAWG API; key байхгүй бол **skipped** | секунд |
| load_business_workbooks | 3 Excel-ийг шалгаж ачаална | < 1 мин |
| reconcile_source_bronze | source ба Bronze мөрийн тоо | секунд |
| dbt_build | seed, snapshot, 43 model, ~176 тест | 2–5 мин |
| collect_dbt_results | dbt-ийн үр дүнг түүхэнд хадгална | секунд |
| monitor_data_quality | freshness, volume, баганын тоо | < 1 мин |
| check_and_alert | 14 дүрэм → alert/✅ | секунд |

**Амжилт:** бүх task `success` (ingest_rawg `skipped` байж болно), `check_and_alert` ногоон.

---

## 9. Үр дүнг шалгах

```bash
psqla() { docker compose exec -T postgres-analytics psql -U "$POSTGRES_USER" -d "$ANALYTICS_DB" -c "$1"; }
psqla "select 'bronze.sessions', count(*) from bronze.sessions union all
       select 'gold.fact_sessions', count(*) from gold.fact_sessions union all
       select 'gold.mart_daily_kpis', count(*) from gold.mart_daily_kpis"
psqla "select resource_type, status, count(*) from monitoring.dbt_node_results
       where invocation_id = (select invocation_id from monitoring.dbt_node_results order by generated_at desc limit 1)
       group by 1, 2 order by 1, 2"
psqla "select table_name, status, source_rows, bronze_rows from monitoring.reconciliation
       where checked_at = (select max(checked_at) from monitoring.reconciliation)"
psqla "select months_since_first_session, round(avg(retention_rate), 3)
       from gold.mart_player_retention where months_since_first_session <= 6 group by 1 order by 1"
psqla "select round(sum(campaign_revenue_usd)) as during, round(sum(baseline_revenue_usd)) as before
       from gold.mart_campaign_performance"
run_project -m monitoring.alerts --dry-run
```

**Амжилт:**
- Bronze ба fact_sessions-ийн тоо 100 000 орчим ба ижил.
- dbt: бүх model `success`, тестүүд `pass` (`warn` байж болно, `fail` байх ёсгүй).
- Reconciliation: 8 хүснэгт бүгд `match`.
- Retention 1.0 → ~0.5 → … буурна; campaign-ий `during` нь `before`-оос их.
- `alerts --dry-run` → `all checks passed` (эсвэл зөвхөн warn).

---

## 10. Dashboard ба UI-д нэвтрэх (SSH tunnel)

```bash
docker compose --profile dashboard up -d --build --wait dashboard
```

Өөрийн компьютер дээр (PowerShell):

```powershell
ssh -N -L 8080:127.0.0.1:8080 -L 8501:127.0.0.1:8501 -L 8585:127.0.0.1:8585 <user>@<vps-ip>
```

Дараа нь браузерт: Airflow `http://localhost:8080` (admin + алхам 7-ийн нууц үг),
Dashboard `http://localhost:8501`.

**Амжилт:** Dashboard-ын 7 таб бүгд өгөгдөлтэй; "Pipeline health" табд өнөөдрийн quality шалгалт.

---

## 11. Цаг тутмын урсгалыг шалгах

`gaming_source_data_generator` цаг бүрийн 00 минутад ажиллана. Хүлээхгүйгээр:

```bash
docker compose exec airflow airflow dags trigger gaming_source_data_generator
```

```bash
docker compose exec -T postgres-source psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
  select date_trunc('hour', started_at) as hour, count(*) from sessions
  where started_at > now() - interval '3 hours' group by 1 order by 1"
```

Дараа нь өдрийн pipeline-ийг дахин trigger хийвэл Meltano **зөвхөн шинэ мөрүүдийг** татаж,
dbt incremental fact-уудад нэмнэ (`sync_source_to_bronze` анхныхаас хамаагүй хурдан).

**Амжилт:** сүүлийн цагт шинэ session-ууд; дараагийн pipeline-ийн дараа `gold.fact_sessions`
тоо өсөж, reconciliation дахин `match`.

---

## 12. Alert-ийн гинжийг шалгах

```bash
docker compose exec airflow airflow dags trigger alert_drill
psqla "select fired_at, source, severity, check_name, object, sent from monitoring.alert_log order by alert_id desc limit 5"
```

**Амжилт:** `alert_log`-д `task_failed · alert_drill` мөр. SMTP тохируулсан бол имэйл ирж,
`sent = t`. Тохируулаагүй бол `sent = f` (зөвхөн log-д).

---

## 13. OpenMetadata (RAM 8 GB+ бол)

```bash
docker compose --profile openmetadata up -d --wait openmetadata-server     # 3–5 мин
```

1. UI (`http://localhost:8585`, admin@open-metadata.org / admin) → Settings → Bots →
   ingestion-bot → token-ийг хуулж `.env`-д `OPENMETADATA_JWT_TOKEN=` болгоно, дараа нь `set -a; . ./.env; set +a`.
2. Ingestion:

```bash
docker compose --profile openmetadata run --rm openmetadata-ingestion
docker compose --profile openmetadata run --rm --entrypoint python openmetadata-ingestion /opt/metadata/create_glossary.py
mkdir -p dbt/target && chmod a+rwx dbt/target          # airflow (uid 50000) бичих эрх
DBT_TARGET_PATH=/opt/project/dbt/target run_dbt docs generate
docker compose --profile openmetadata run --rm --entrypoint metadata openmetadata-ingestion ingest-dbt -c /project/dbt
```

**Амжилт:** `gold.mart_daily_kpis` дээр description, Tier1, `GamingAnalytics.DAU` glossary,
lineage (silver → gold); `silver.stg_players.username` дээр `DataGovernance.pii` tag.

---

## Асуудал гарвал

| Шинж | Шалгах |
| --- | --- |
| Task улаан | UI → task → Logs, **эхний** алдааг уншина; `monitoring.alert_log` |
| `dbt_build` улаан | `psqla "select node_name, status, message from monitoring.dbt_node_results where status in ('error','fail') order by generated_at desc limit 10"` |
| Meltano алдаа | `docker compose --profile orchestration build --no-cache airflow` |
| Dashboard "permission denied" | `dbt_build` нэг удаа амжилттай ажилласан эсэх (Gold-д grant өгдөг) |
| Санах ой дутуу (OOM) | `docker stats`; OpenMetadata-г зогсооно: `docker compose --profile openmetadata stop` |

Бүгдийг зогсоох (өгөгдөл хадгалагдана): `docker compose --profile orchestration --profile dashboard --profile openmetadata stop`.
Бүгдийг устгаж эхнээс нь: `docker compose --profile orchestration --profile dashboard --profile openmetadata down -v` (**бүх өгөгдөл устна**).
