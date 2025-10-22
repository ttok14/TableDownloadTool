rem 가상 환경 활성화 (현재 프로젝트의 가상 환경 경로로 수정)
call .venv\Scripts\activate.bat

rem 가상 환경 활성화 후 PyInstaller 실행
pyinstaller app.spec

rem (선택 사항) 빌드 후 가상 환경 비활성화
deactivate

pause