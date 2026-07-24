@echo off
chcp 65001 >nul
title Ingress Service

REM ============================================================
REM  配置区域
REM  数据库/Redis 连接信息已在 jar 同目录的 application-dev.yml 中配置，
REM  Spring Boot 会自动加载外部配置文件，无需命令行传参。
REM ============================================================
set INGRESS_JAR=C:\dev\ingress\trade-ingress-1.0.0-SNAPSHOT.jar
set JVM_OPTS=-Xms512m -Xmx1024m -XX:+UseG1GC -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai

REM ============================================================
REM  前置检查
REM ============================================================
where java >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未找到 java 命令，请确保 JDK 17 已安装并加入系统 PATH。
    pause
    exit /b 1
)

if not exist "%INGRESS_JAR%" (
    echo [错误] Ingress jar 不存在：%INGRESS_JAR%
    pause
    exit /b 1
)

REM 确认外部配置文件存在
if not exist "%~dp0application.yml" (
    echo [警告] 未找到 jar 同目录的 application.yml，将使用 jar 包内默认配置。
)
if not exist "%~dp0application-dev.yml" (
    echo [警告] 未找到 jar 同目录的 application-dev.yml，将使用 jar 包内默认配置。
)

REM ============================================================
REM  启动 Ingress 服务
REM ============================================================
echo =============================================
echo  正在启动 Ingress 服务...
echo  外部配置目录：%~dp0
echo =============================================
cd /d "%~dp0"
java %JVM_OPTS% -jar "%INGRESS_JAR%"

REM 进程退出后窗口不关闭，便于查看错误信息
echo.
echo =============================================
echo  Ingress 服务已停止
echo =============================================
pause
