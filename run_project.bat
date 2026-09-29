@echo off
setlocal enabledelayedexpansion
title Cold-Chain Medicine Monitoring System - Academic Runner

:menu
cls
echo ===============================================================================
echo        COLD-CHAIN MEDICINE MONITORING USING SPARK DATAFRAMES
echo        Academic Case-Study Topic: 5.2 Spark DataFrames
echo ===============================================================================
echo.
echo  --- ACADEMIC DATAFRAME PIPELINE ---
echo  [1] Step 1: Run Pandas Data Preparation (Load, Inspect, Clean CSV)
echo  [2] Step 2: Run Main PySpark DataFrame Processing (Academic Core)
echo.
echo  --- REAL-TIME APPLICATION EXTENSION (DOCKER) ---
echo  [3] Step 3: Start Docker Infrastructure (Kafka, Postgres, Spark, Grafana)
echo  [4] Step 4: Check Docker Container Status
echo  [5] Step 5: Start PySpark Structured Streaming Job (Run ONCE)
echo  [6] Step 6: Start Real-Time IoT Telemetry Producer
echo  [7] Step 7: Inspect Processed Records in PostgreSQL
echo  [8] Step 8: Open Grafana Dashboard (http://localhost:3001)
echo.
echo  --- MAINTENANCE / TROUBLESHOOTING ---
echo  [9] Clear Spark Streaming Checkpoint (/tmp/cold-chain-checkpoint)
echo  [10] Stop Docker Infrastructure (docker compose down)
echo  [11] Exit
echo ===============================================================================
set /p choice="Enter your choice (1-11): "

if "%choice%"=="1" goto run_pandas
if "%choice%"=="2" goto run_spark_batch
if "%choice%"=="3" goto docker_up
if "%choice%"=="4" goto docker_status
if "%choice%"=="5" goto run_spark_stream
if "%choice%"=="6" goto run_producer
if "%choice%"=="7" goto check_postgres
if "%choice%"=="8" goto open_grafana
if "%choice%"=="9" goto clear_checkpoint
if "%choice%"=="10" goto docker_down
if "%choice%"=="11" goto end

echo Invalid selection. Please choose 1-11.
pause
goto menu

:run_pandas
cls
echo ===============================================================================
echo [STEP 1] Running Pandas Data Preparation...
echo Command: python pandas\data_preparation.py
echo ===============================================================================
python pandas\data_preparation.py
echo.
pause
goto menu

:run_spark_batch
cls
echo ===============================================================================
echo [STEP 2] Running Main PySpark DataFrame Processing...
echo Command: python spark\dataframe_processing.py
echo ===============================================================================
python spark\dataframe_processing.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo [*] Note: If local Windows PySpark is not installed, executing inside Docker:
    echo Command: docker exec -it cc-spark-master spark-submit /opt/spark/dataframe_processing.py
    docker exec -it cc-spark-master spark-submit /opt/spark/dataframe_processing.py
)
echo.
pause
goto menu

:docker_up
cls
echo ===============================================================================
echo [STEP 3] Starting Infrastructure Containers with Docker Compose...
echo Command: docker compose up -d
echo ===============================================================================
docker compose up -d
echo.
echo [*] Waiting 5 seconds for services to initialize...
timeout /t 5 /nobreak >nul
docker compose ps
echo.
pause
goto menu

:docker_status
cls
echo ===============================================================================
echo [STEP 4] Docker Container Health & Ports
echo Command: docker compose ps
echo ===============================================================================
docker compose ps
echo.
pause
goto menu

:run_spark_stream
cls
echo ===============================================================================
echo [STEP 5] Starting PySpark Structured Streaming Pipeline (RUN ONCE)...
echo Command:
echo docker exec -it cc-spark-master spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --master spark://cc-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 /opt/spark/cold_chain_stream.py
echo ===============================================================================
echo [*] Launching Spark Streaming Job... Press Ctrl+C in this terminal to stop streaming.
docker exec -it cc-spark-master spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --master spark://cc-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 /opt/spark/cold_chain_stream.py
echo.
pause
goto menu

:run_producer
cls
echo ===============================================================================
echo [STEP 6] Starting Real-Time Telemetry Producer...
echo Command: python producer\producer.py
echo ===============================================================================
python producer\producer.py
echo.
pause
goto menu

:check_postgres
cls
echo ===============================================================================
echo [STEP 7] Inspecting PostgreSQL Database Records...
echo ===============================================================================
echo [Sensor Events Count]:
docker exec -it cc-postgres psql -U postgres -d coldchain -c "SELECT COUNT(*) AS total_readings FROM sensor_events;"
echo [Temperature Alerts Count]:
docker exec -it cc-postgres psql -U postgres -d coldchain -c "SELECT condition, alert_type, COUNT(*) FROM temperature_alerts GROUP BY condition, alert_type;"
echo [Latest 5 Events]:
docker exec -it cc-postgres psql -U postgres -d coldchain -c "SELECT event_id, shipment_id, medicine, temperature, condition, alert_type FROM sensor_events ORDER BY id DESC LIMIT 5;"
echo.
pause
goto menu

:open_grafana
cls
echo ===============================================================================
echo [STEP 8] Opening Grafana Dashboard in Browser...
echo URL: http://localhost:3001
echo Credentials: admin / admin
echo ===============================================================================
start http://localhost:3001
echo.
pause
goto menu

:clear_checkpoint
cls
echo ===============================================================================
echo [MAINTENANCE] Clearing Spark Streaming Checkpoint...
echo Command:
echo docker exec cc-spark-master rm -rf /tmp/cold-chain-checkpoint
echo ===============================================================================
docker exec cc-spark-master rm -rf /tmp/cold-chain-checkpoint
echo [SUCCESS] Checkpoint removed. You may now restart the streaming query.
echo.
pause
goto menu

:docker_down
cls
echo ===============================================================================
echo [STEP 10] Stopping Docker Infrastructure...
echo Command: docker compose down
echo ===============================================================================
docker compose down
echo.
pause
goto menu

:end
exit /b 0
