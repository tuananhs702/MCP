import sys
import os
import asyncio
from flask import Flask, render_template, request, jsonify, send_file, url_for, session, redirect
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mcp_client.client import MCPClient

app = Flask(__name__)
app.secret_key = 'mcp_secret_key'  # Add secret key for session management

# Đường dẫn tới file server MCP
SERVER_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                 "weather", "mcp_server.py")

# Khởi tạo client MCP
client = None

# Tạo event loop riêng cho ứng dụng
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

def initialize_client():
    global client
    if client is None:
        client = MCPClient()
        loop.run_until_complete(client.connect_to_server(SERVER_SCRIPT_PATH))
    return client

# Khởi tạo client khi ứng dụng khởi động
@app.before_first_request
def setup_client():
    initialize_client()

# Add this route to your existing Flask app
@app.route('/')
def home():
    return render_template('home.html')

# Make sure your existing /query route can handle AJAX requests
@app.route('/query', methods=['POST'])
def query():
    # Your existing query handling code
    # ...
    
    # If it's an AJAX request, return just the chat history
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('chat_response.html', chat_history=chat_history)
    
    # Otherwise return the full page as before
    return render_template('index.html', chat_history=chat_history)

@app.route('/query', methods=['POST'])
def process_query():
    global client
    
    if client is None:
        initialize_client()
    
    query = request.form.get('query', '')
    
    # Xử lý query bất đồng bộ
    response = loop.run_until_complete(client.process_query(query))
    
    # Xử lý response để thay thế link_to_your_file.xlsx bằng URL thực
    if "(link_to_your_file.xlsx)" in response:
        excel_path = os.path.join(app.static_url_path, 'files', 'sample_data.xlsx')
        download_url = url_for('download_excel', filename='sample_data.xlsx')
        response = response.replace("(link_to_your_file.xlsx)", f'<a href="{download_url}" target="_blank">Tải xuống</a>')
    
    # Save to chat history
    if 'chat_history' not in session:
        session['chat_history'] = []
    
    session['chat_history'].append({
        'query': query,
        'response': response
    })
    session.modified = True
    
    # Check if we should continue the chat
    continue_chat = request.form.get('continue_chat', 'false') == 'true'
    
    if continue_chat:
        return redirect(url_for('index'))
    else:
        return render_template('results.html', query=query, response=response)

@app.route('/api/query', methods=['POST'])
def api_query():
    global client
    
    if client is None:
        initialize_client()
    
    data = request.get_json()
    query = data.get('query', '')
    
    # Xử lý query bất đồng bộ
    response = loop.run_until_complete(client.process_query(query))
    
    # Xử lý response để thay thế link_to_your_file.xlsx bằng URL thực
    if "(link_to_your_file.xlsx)" in response:
        download_url = url_for('download_excel', filename='sample_data.xlsx')
        response = response.replace("(link_to_your_file.xlsx)", f'<a href="{download_url}" target="_blank">Tải xuống</a>')
    
    # Save to chat history if needed
    if data.get('save_history', True):
        if 'chat_history' not in session:
            session['chat_history'] = []
        
        session['chat_history'].append({
            'query': query,
            'response': response
        })
        session.modified = True
    
    return jsonify({'response': response})

# Route to clear chat history
@app.route('/clear_history', methods=['POST'])
def clear_history():
    session['chat_history'] = []
    return redirect(url_for('index'))

# Thêm route mới để tải xuống file Excel
@app.route('/download/<filename>')
def download_excel(filename):
    # Đường dẫn đến thư mục chứa file Excel
    excel_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'files')
    # Đảm bảo thư mục tồn tại
    os.makedirs(excel_dir, exist_ok=True)
    # Đường dẫn đầy đủ đến file
    file_path = os.path.join(excel_dir, filename)
    
    # Nếu file không tồn tại, tạo một file Excel mẫu
    if not os.path.exists(file_path):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        # Thêm tiêu đề
        ws.append(["Họ và tên", "Năm sinh", "Quê quán", "Số điện thoại", "Sở thích"])
        # Thêm dữ liệu mẫu
        # ws.append(["Trần Tuấn Anh", "29/01/2002", "Gia Lai", "396196208", "Chơi game"])
        # ws.append(["Tạ Quốc Việt", "15/05/1998", "Hà Nội", "387654321", "Đọc sách"])
        # Lưu file
        wb.save(file_path)
    
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
    # app.run(debug=True, port=2999)