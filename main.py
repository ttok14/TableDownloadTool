import sys
import os
import json
import pandas as pd
import os;

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QLabel, QPlainTextEdit, QFileDialog, QMessageBox
)
from PySide6.QtCore import QThread, Signal, Slot

# Google API 관련 라이브러리
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- 다크 테마 스타일시트 ---
DARK_THEME_QSS = """
    QWidget {
        background-color: #2b2b2b;
        color: #f0f0f0;
        font-family: Arial;
    }
    QMainWindow {
        background-color: #2b2b2b;
    }
    QLineEdit {
        background-color: #3c3f41;
        border: 1px solid #555;
        border-radius: 4px;
        padding: 5px;
        color: #f0f0f0;
    }
    QPushButton {
        background-color: #555;
        border: 1px solid #666;
        border-radius: 4px;
        padding: 5px;
    }
    QPushButton:hover {
        background-color: #666;
    }
    QPushButton:pressed {
        background-color: #777;
    }
    QPlainTextEdit {
        background-color: #3c3f41;
        border: 1px solid #555;
        border-radius: 4px;
        color: #f0f0f0;
        font-size: 10pt;
    }
    QLabel {
        color: #f0f0f0;
    }
    QMessageBox {
        background-color: #2b2b2b;
    }
"""

# --- Google API 인증 및 통신을 담당하는 클래스 ---
class GoogleAPIClient:
    """Handles authentication and communication with Google Drive & Sheets APIs."""
    SCOPES = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
    ]
    
    def __init__(self):
        self.creds = None
        self.drive_service = None
        self.sheets_service = None

    def authenticate(self):
        """
        사용자 인증을 수행하고 API 서비스 객체를 생성합니다.
        성공 시 None, 실패 시 에러 메시지를 반환합니다.
        """
        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)
        
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                try:
                    self.creds.refresh(Request())
                except Exception as e:
                    if os.path.exists('token.json'):
                        os.remove('token.json')
                    return self.authenticate()
            else:
                if not os.path.exists('credentials.json'):
                    return "인증 실패: 'credentials.json' 파일을 찾을 수 없습니다."
                
                try:
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.SCOPES)
                    self.creds = flow.run_local_server(port=0)
                except Exception as e:
                    return f"인증 과정에서 오류가 발생했습니다: {e}"

            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())
        
        try:
            self.drive_service = build('drive', 'v3', credentials=self.creds)
            self.sheets_service = build('sheets', 'v4', credentials=self.creds)
            return None # 성공
        except HttpError as e:
            return f"API 서비스 빌드 실패: {e}"

    def get_spreadsheets_recursively(self, root_folder_id):
        """
        지정된 폴더와 모든 하위 폴더를 재귀적으로 탐색하여
        모든 구글 스프레드시트 파일의 (ID, 이름) 리스트를 생성(yield)합니다.
        """
        folders_to_process = [root_folder_id]
        processed_folders = set()

        while folders_to_process:
            current_folder_id = folders_to_process.pop(0)
            if current_folder_id in processed_folders:
                continue
            
            processed_folders.add(current_folder_id)
            
            try:
                page_token = None
                while True:
                    query = (f"'{current_folder_id}' in parents and trashed=false")
                    response = self.drive_service.files().list(
                        q=query, spaces='drive',
                        fields='nextPageToken, files(id, name, mimeType)',
                        pageToken=page_token
                    ).execute()

                    files_in_folder = response.get('files', [])
                    spreadsheets = []
                    for f in files_in_folder:
                        if f.get('mimeType') == 'application/vnd.google-apps.folder':
                            folders_to_process.append(f.get('id'))
                        elif f.get('mimeType') == 'application/vnd.google-apps.spreadsheet':
                            spreadsheets.append({'id': f.get('id'), 'name': f.get('name')})
                    
                    if spreadsheets:
                        yield {'type': 'files', 'data': spreadsheets}

                    page_token = response.get('nextPageToken', None)
                    if page_token is None:
                        break
            except HttpError as error:
                yield {'type': 'log', 'data': f"폴더 접근 오류 (ID: {current_folder_id}): {error}"}

    def get_sheet_info(self, spreadsheet_id):
        """스프레드시트의 모든 시트(탭) 이름을 리스트로 반환합니다."""
        try:
            sheet_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id, fields='sheets.properties.title'
            ).execute()
            sheets = sheet_metadata.get('sheets', [])
            return [sheet.get('properties', {}).get('title') for sheet in sheets]
        except HttpError as error:
            print(f"시트 정보 가져오기 오류 (ID: {spreadsheet_id}): {error}")
            return None


# --- 다운로드 작업을 수행하는 QThread 워커 ---
class DownloadWorker(QThread):
    log_message = Signal(str, str)  # 메시지와 색상을 함께 전달하도록 변경
    process_finished = Signal(str)

    # 로그 색상 정의
    COLOR_INFO = "#d0d0d0"
    COLOR_SUCCESS = "#00e676"
    COLOR_ERROR = "#ff5252"
    COLOR_WARN = "#ffd740"
    COLOR_HIGHLIGHT = "#40c4ff"
    COLOR_SYSTEM = "#a9a9a9"

    def __init__(self, root_folder_id, save_path):
        super().__init__()
        self.root_folder_id = root_folder_id
        self.save_path = save_path
        self.g_client = GoogleAPIClient()

    def clear_save_directory(self):
        self.log_message.emit(f"'{self.save_path}' 폴더의 기존 파일들을 삭제합니다...", self.COLOR_SYSTEM)
        try:
            for filename in os.listdir(self.save_path):
                file_path = os.path.join(self.save_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            self.log_message.emit("폴더 정리 완료.", self.COLOR_SYSTEM)
        except Exception as e:
            self.log_message.emit(f"폴더 정리 중 오류 발생: {e}", self.COLOR_ERROR)

    def run(self):
        self.log_message.emit("Google API 인증을 시작합니다...", self.COLOR_INFO)
        auth_error = self.g_client.authenticate()
        if auth_error:
            self.process_finished.emit(f"인증 실패: {auth_error}")
            return
        self.log_message.emit("인증 성공.", self.COLOR_SUCCESS)

        self.clear_save_directory()

        self.log_message.emit(f"\n루트 폴더({self.root_folder_id})에서 스프레드시트 파일을 검색합니다...", self.COLOR_INFO)
        
        total_sheets_downloaded = 0
        
        try:
            all_files = []
            file_generator = self.g_client.get_spreadsheets_recursively(self.root_folder_id)
            for result in file_generator:
                if result['type'] == 'log':
                    self.log_message.emit(result['data'], self.COLOR_ERROR)
                elif result['type'] == 'files':
                    all_files.extend(result['data'])

            if not all_files:
                self.process_finished.emit("완료: 다운로드할 스프레드시트 파일을 찾지 못했습니다.")
                return

            total_files = len(all_files)
            self.log_message.emit(f"총 {total_files}개의 스프레드시트 파일을 찾았습니다.", self.COLOR_INFO)

            for i, file in enumerate(all_files):
                file_id, file_name = file['id'], file['name']
                self.log_message.emit(f"\n[{i+1}/{total_files}] 처리 중: '{file_name}'", self.COLOR_HIGHLIGHT)

                all_sheet_names = self.g_client.get_sheet_info(file_id)
                if all_sheet_names is None:
                    self.log_message.emit(f"-> '{file_name}' 파일의 시트 정보를 가져올 수 없습니다. 건너뜁니다.", self.COLOR_WARN)
                    continue

                sheets_to_process = ['Table', 'Schema']
                found_sheets = False

                for sheet_name in sheets_to_process:
                    if sheet_name in all_sheet_names:
                        found_sheets = True
                        self.log_message.emit(f"-> '{sheet_name}' 시트 다운로드 시도...", self.COLOR_INFO)
                        try:
                            url = f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
                            df = pd.read_csv(url)
                            
                            safe_file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
                            output_filename = f"{safe_file_name}_{sheet_name}.csv"
                            output_filepath = os.path.join(self.save_path, output_filename)

                            df.to_csv(output_filepath, index=False, encoding='utf-8-sig')
                            self.log_message.emit(f"   -> 저장 완료: {output_filepath}", self.COLOR_SUCCESS)
                            total_sheets_downloaded += 1
                        except Exception as e:
                            self.log_message.emit(f"   -> 다운로드 실패: {sheet_name} (파일: {file_name})", self.COLOR_ERROR)
                            self.log_message.emit(f"   -> 오류: {e}", self.COLOR_ERROR)
                            self.log_message.emit("   -> 시트 이름 컨벤션이나 시트 내용을 확인해주세요.", self.COLOR_ERROR)
                
                if not found_sheets:
                    self.log_message.emit(f"-> '{file_name}' 파일에 'Table' 또는 'Schema' 시트를 찾을 수 없습니다. (발견된 시트: {all_sheet_names})", self.COLOR_WARN)

        except Exception as e:
            self.process_finished.emit(f"작업 중 심각한 오류 발생: {e}")
            return
            
        self.process_finished.emit(f"모든 작업 완료! 총 {total_sheets_downloaded}개의 시트를 다운로드했습니다.")

        try:
            os.startfile(self.save_path)
            print(f"탐색기에서 경로를 열었습니다: {self.save_path}")
        except FileNotFoundError:
            print(f"오류: 경로를 찾을 수 없습니다: {self.save_path}")
        except Exception as e:
            print(f"오류 발생: {e}")


# --- 메인 윈도우 클래스 ---
class MainWindow(QMainWindow):
    CONFIG_FILE = "config.json"

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Google Drive CSV Downloader")
        self.setGeometry(100, 100, 700, 500)

        # --- 위젯 생성 ---
        self.folder_id_label = QLabel("Google Drive 폴더 ID:")
        self.folder_id_input = QLineEdit()
        self.folder_id_input.setPlaceholderText("https://drive.google.com/drive/folders/여기에_있는_ID_입력")

        self.save_path_label = QLabel("저장 경로:")
        self.save_path_input = QLineEdit()
        self.save_path_input.setReadOnly(True)
        self.save_path_button = QPushButton("경로 선택")

        self.start_button = QPushButton("다운로드 시작")
        self.start_button.setStyleSheet("font-weight: bold; padding: 8px; background-color: #0078d7;")

        self.log_display = QPlainTextEdit()
        self.log_display.setReadOnly(True)

        # --- 레이아웃 설정 ---
        main_layout = QVBoxLayout()
        form_layout = QHBoxLayout()
        form_layout.addWidget(self.folder_id_label)
        form_layout.addWidget(self.folder_id_input)
        main_layout.addLayout(form_layout)

        path_layout = QHBoxLayout()
        path_layout.addWidget(self.save_path_label)
        path_layout.addWidget(self.save_path_input)
        path_layout.addWidget(self.save_path_button)
        main_layout.addLayout(path_layout)

        main_layout.addWidget(self.start_button)
        main_layout.addWidget(QLabel("진행 상황 로그:"))
        main_layout.addWidget(self.log_display)

        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

        # --- 시그널 및 슬롯 연결 ---
        self.save_path_button.clicked.connect(self.select_save_path)
        self.start_button.clicked.connect(self.start_download)
        
        self.worker = None
        self.load_settings()

    def load_settings(self):
        try:
            if os.path.exists(self.CONFIG_FILE):
                with open(self.CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.folder_id_input.setText(config.get("folder_id", ""))
                    self.save_path_input.setText(config.get("save_path", ""))
        except (json.JSONDecodeError, IOError) as e:
            print(f"설정 파일 로드 오류: {e}")

    def save_settings(self):
        config = {
            "folder_id": self.folder_id_input.text(),
            "save_path": self.save_path_input.text()
        }
        try:
            with open(self.CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=4)
        except IOError as e:
            print(f"설정 파일 저장 오류: {e}")

    @Slot()
    def select_save_path(self):
        directory = QFileDialog.getExistingDirectory(self, "저장할 폴더를 선택하세요")
        if directory:
            self.save_path_input.setText(directory)

    @Slot()
    def start_download(self):
        root_folder_id = self.folder_id_input.text().strip()
        save_path = self.save_path_input.text().strip()

        if not root_folder_id:
            QMessageBox.warning(self, "입력 오류", "Google Drive 폴더 ID를 입력해주세요.")
            return
        if not save_path:
            QMessageBox.warning(self, "입력 오류", "CSV 파일을 저장할 경로를 선택해주세요.")
            return

        self.log_display.clear()
        self.start_button.setEnabled(False)
        self.start_button.setText("다운로드 중...")
        
        self.save_settings()

        self.worker = DownloadWorker(root_folder_id, save_path)
        self.worker.log_message.connect(self.append_log)
        self.worker.process_finished.connect(self.on_finished)
        self.worker.start()

    @Slot(str, str)
    def append_log(self, message, color):
        # HTML을 사용하여 텍스트 색상 지정
        html = f'<p style="color:{color}; margin: 0; white-space: pre-wrap;">{message.replace(" ", "&nbsp;")}</p>'
        self.log_display.appendHtml(html)
        self.log_display.verticalScrollBar().setValue(self.log_display.verticalScrollBar().maximum())

    @Slot(str)
    def on_finished(self, message):
        color = self.worker.COLOR_ERROR if "실패" in message or "오류" in message else self.worker.COLOR_SUCCESS
        self.append_log(f"\n{message}", color)
        QMessageBox.information(self, "작업 완료", message)
        self.start_button.setEnabled(True)
        self.start_button.setText("다운로드 시작")
        self.worker = None

    def closeEvent(self, event):
        self.save_settings()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyleSheet(DARK_THEME_QSS)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

