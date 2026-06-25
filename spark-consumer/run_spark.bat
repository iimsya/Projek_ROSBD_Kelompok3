@echo off
REM Mencari folder instalasi Microsoft JDK 17 secara otomatis
for /d %%i in ("C:\Program Files\Microsoft\jdk-17*") do set "JAVA_HOME=%%i"
for /d %%i in ("C:\Program Files\Eclipse Adoptium\jdk-17*") do set "JAVA_HOME=%%i"
if "%JAVA_HOME%"=="" (
    echo Java 17 belum selesai diinstal atau foldernya tidak ditemukan.
    pause
    exit /b 1
)

echo Menggunakan JAVA_HOME: %JAVA_HOME%
set "HADOOP_HOME=D:\hadoop"
set "PATH=%HADOOP_HOME%\bin;%PATH%"

REM Menjalankan PySpark
python stream_processor.py
pause
 