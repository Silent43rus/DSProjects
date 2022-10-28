@ECHO OFF
cd %TMP%
set PATH=%PATH%;%USERPROFILE%\AppData\Roaming\Python\Python38\Scripts
set PATH=%PATH%;%USERPROFILE%\AppData\Roaming\Python\Python39\Scripts
python %~dp0updater.py