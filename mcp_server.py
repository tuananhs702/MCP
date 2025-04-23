import aiomysql
from mcp.server.fastmcp import FastMCP
from typing import Any
import httpx
import os
import openpyxl
from aiofiles import open as aio_open
from fastapi import UploadFile
import json
import base64
import pandas as pd
from datetime import datetime, timedelta


# Khởi tạo server FastMCP
mcp = FastMCP("Select_mysql")

# Constants
NWS_API_BASE = "https://api.weather.gov"
USER_AGENT = "weather-app/1.0"
GITHUB_API_BASE = "https://api.github.com"

# Hàm lấy dữ liệu từ bảng thanhvien trong MySQL (Sử dụng aiomysql)
async def get_mysql_data(table: str = "thanhvien", hoten: str = None):
    try:
        conn = await aiomysql.connect(
            host="localhost",
            user="root",
            password="",
            db="ql_claude",
            connect_timeout=5
        )
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            if hoten:
                await cursor.execute(f"SELECT * FROM {table} WHERE hoten = %s", (hoten,))
            else:
                await cursor.execute(f"SELECT * FROM {table}")
            rows = await cursor.fetchall()
            
            # Nếu không có kết quả, trả về danh sách rỗng
            if not rows:
                return []  # Không trả về None
            return rows
    except Exception as e:
        return f"Lỗi kết nối đến MySQL: {str(e)}"

async def get_coordinates(city: str) -> tuple[float, float] | None:
    """Lấy tọa độ từ tên thành phố thông qua OpenStreetMap Nominatim."""
    url = f"https://nominatim.openstreetmap.org/search?format=json&q={city}, Vietnam"
    headers = {"User-Agent": USER_AGENT}

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=15.0)
            response.raise_for_status()
            data = response.json()
            if not data:
                return None
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            return lat, lon
        except Exception:
            return None
        
async def make_nws_request(url: str) -> dict[str, Any] | None:
    """Make a request to the NWS API with proper error handling."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/geo+json"
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception:
            return None
        
def format_alert(feature: dict) -> str:
    """Format an alert feature into a readable string."""
    props = feature["properties"]
    return f"""
Event: {props.get('event', 'Unknown')}
Area: {props.get('areaDesc', 'Unknown')}
Severity: {props.get('severity', 'Unknown')}
Description: {props.get('description', 'No description available')}
Instructions: {props.get('instruction', 'No specific instructions provided')}
"""

@mcp.tool()
async def get_member_by_name(hoten: str) -> str:
    """Tìm kiếm thành viên theo họ tên từ bảng MySQL."""
    try:
        # Lấy dữ liệu từ MySQL
        data = await get_mysql_data(hoten=hoten)

        if isinstance(data, str):
            return data  # Trường hợp lỗi kết nối hoặc vấn đề khác

        if not data:
            return f"Không tìm thấy thành viên có tên {hoten}."

        # Trả về thông tin của thành viên tìm thấy
        member = data[0]  # Chỉ lấy thành viên đầu tiên tìm thấy
        return (
            f"Thông tin thành viên:\n"
            f"Họ tên: {member.get('hoten', 'Không có')}\n"
            f"Năm sinh: {member.get('namsinh', 'Không có')}\n"
            f"Quê quán: {member.get('quequan', 'Không có')}\n"
            f"SĐT: {member.get('sdt', 'Không có')}"
        )

    except Exception as e:
        return f"Lỗi khi truy vấn dữ liệu MySQL: {str(e)}"


# Hàm lấy thông tin từ github
async def fetch_github_data(repo_owner: str, repo_name: str, file_path: str) -> dict:
    """Lấy dữ liệu từ GitHub repository."""
    url = f"{GITHUB_API_BASE}/repos/{repo_owner}/{repo_name}/contents/{file_path}"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github.v3+json"
    }
    
    # Thêm token GitHub nếu có (để tăng giới hạn request)
    github_token = os.getenv("GITHUB_TOKEN")
    if github_token:
        headers["Authorization"] = f"token {github_token}"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=15.0)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                # Đây là thư mục
                return {"type": "directory", "items": data}
            elif "content" in data and data.get("encoding") == "base64":
                # Đây là file có nội dung
                content = base64.b64decode(data["content"]).decode("utf-8")
                return {"type": "file", "content": content, "name": data.get("name", "")}
            else:
                return {"type": "unknown", "data": data}
                
        except Exception as e:
            return {"type": "error", "message": str(e)}

async def phan_tich_du_lieu(data: str, file_type: str) -> str:
    """Phân tích dữ liệu và đưa ra dự báo dựa trên loại file."""
    try:
        if file_type.endswith('.csv'):
            # Phân tích dữ liệu CSV
            import io
            df = pd.read_csv(io.StringIO(data))
        elif file_type.endswith('.json'):
            # Phân tích dữ liệu JSON
            json_data = json.loads(data)
            if isinstance(json_data, list):
                df = pd.DataFrame(json_data)
            else:
                return "Định dạng dữ liệu JSON không được hỗ trợ cho dự báo."
        else:
            return f"Loại file {file_type} không được hỗ trợ cho dự báo."
        
        # Kiểm tra xem dataframe có cột thời gian/ngày và cột giá trị không
        date_cols = [col for col in df.columns if any(term in col.lower() for term in ['date', 'time', 'ngay', 'thang', 'nam', 'thoi_gian'])]
        value_cols = [col for col in df.columns if any(term in col.lower() for term in ['value', 'price', 'temp', 'temperature', 'gia_tri', 'nhiet_do', 'gia', 'luong'])]
        
        if not date_cols or not value_cols:
            # Trả về thống kê tóm tắt nếu không phát hiện dữ liệu chuỗi thời gian
            summary = df.describe().to_string()
            return f"Tóm tắt dữ liệu (không phát hiện chuỗi thời gian):\n{summary}"
        
        # Sử dụng cột ngày đầu tiên và cột giá trị đầu tiên cho dự báo
        date_col = date_cols[0]
        value_col = value_cols[0]
        
        # Chuyển đổi sang định dạng datetime nếu cần
        if df[date_col].dtype != 'datetime64[ns]':
            try:
                df[date_col] = pd.to_datetime(df[date_col])
            except:
                return f"Không thể chuyển đổi cột {date_col} sang định dạng datetime."
        
        # Sắp xếp theo ngày
        df = df.sort_values(by=date_col)
        
        # Dự báo đơn giản: tính toán sự thay đổi trung bình và dự đoán tương lai
        if len(df) < 3:
            return "Không đủ điểm dữ liệu để dự báo."
        
        # Tính toán sự thay đổi trung bình
        df['thay_doi'] = df[value_col].diff()
        thay_doi_tb = df['thay_doi'].mean()
        
        # Lấy giá trị và ngày cuối cùng
        gia_tri_cuoi = df[value_col].iloc[-1]
        ngay_cuoi = df[date_col].iloc[-1]
        
        # Dự báo 5 giá trị tiếp theo
        du_bao = []
        for i in range(1, 6):
            ngay_tiep = ngay_cuoi + timedelta(days=i)
            gia_tri_tiep = gia_tri_cuoi + (thay_doi_tb * i)
            du_bao.append(f"{ngay_tiep.strftime('%Y-%m-%d')}: {gia_tri_tiep:.2f}")
        
        # Xác định xu hướng
        xu_huong = "tăng" if thay_doi_tb > 0 else "giảm" if thay_doi_tb < 0 else "ổn định"
        
        ket_qua = f"Phân tích dữ liệu {value_col}:\n"
        ket_qua += f"- Giá trị hiện tại: {gia_tri_cuoi:.2f}\n"
        ket_qua += f"- Thay đổi trung bình hàng ngày: {thay_doi_tb:.2f}\n"
        ket_qua += f"- Xu hướng: {xu_huong}\n\n"
        ket_qua += "Dự báo cho 5 ngày tới:\n"
        ket_qua += "\n".join(du_bao)
        
        return ket_qua
        
    except Exception as e:
        return f"Lỗi khi phân tích dữ liệu: {str(e)}"

@mcp.tool()
async def du_bao_tu_github(repo_owner: str, repo_name: str, file_path: str) -> str:
    """
    Lấy dữ liệu từ GitHub và phân tích để đưa ra dự báo.
    
    Args:
        repo_owner: Tên chủ sở hữu repository (username hoặc tổ chức)
        repo_name: Tên repository
        file_path: Đường dẫn đến file dữ liệu trong repository (hỗ trợ .csv, .json)
    """
    # Lấy file từ GitHub
    ket_qua = await fetch_github_data(repo_owner, repo_name, file_path)
    
    if ket_qua["type"] == "error":
        return f"Lỗi khi lấy dữ liệu từ GitHub: {ket_qua['message']}"
    
    if ket_qua["type"] == "directory":
        # Liệt kê các file trong thư mục
        files = [item["name"] for item in ket_qua["items"] if item.get("type") != "dir"]
        return f"Đường dẫn '{file_path}' là một thư mục. Các file có sẵn:\n" + "\n".join(files)
    
    if ket_qua["type"] == "file":
        # Kiểm tra xem file có được hỗ trợ để dự báo không
        file_name = ket_qua["name"]
        if file_name.endswith(('.csv', '.json')):
            du_bao = await phan_tich_du_lieu(ket_qua["content"], file_name)
            return f"Dữ liệu từ file {file_name} trong repository {repo_owner}/{repo_name}:\n\n{du_bao}"
        else:
            # Chỉ trả về nội dung cho các file không được hỗ trợ
            content_preview = ket_qua["content"][:500] + "..." if len(ket_qua["content"]) > 500 else ket_qua["content"]
            return f"Nội dung file {file_name} (không hỗ trợ dự báo):\n\n{content_preview}"
    
    return "Không thể xử lý dữ liệu từ GitHub."

@mcp.tool()
async def get_vietnam_weather(city: str = "Hanoi") -> str:
    """Lấy thời tiết hiện tại ở Việt Nam (dựa trên thành phố) từ Open-Meteo."""
    coords = await get_coordinates(city)
    if not coords:
        return f"Không thể tìm thấy tọa độ cho '{city}'."

    lat, lon = coords
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, timeout=15.0)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            return f"Lỗi khi lấy dữ liệu thời tiết: {e}"

    current = data.get("current_weather", {})
    if not current:
        return f"Không có dữ liệu thời tiết cho '{city}'."

    temp = current.get("temperature")
    wind = current.get("windspeed")
    weather_code = current.get("weathercode")
    description = f"Nhiệt độ: {temp}°C | Gió: {wind} km/h | Mã thời tiết: {weather_code}"

    return f"📍 Thời tiết tại {city}:\n{description}"

@mcp.tool()
async def get_alerts(state: str) -> str:
    """Get weather alerts for a US state.

    Args:
        state: Two-letter US state code (e.g. CA, NY)
    """
    url = f"{NWS_API_BASE}/alerts/active/area/{state}"
    data = await make_nws_request(url)

    if not data or "features" not in data:
        return "Unable to fetch alerts or no alerts found."

    if not data["features"]:
        return "No active alerts for this state."

    alerts = [format_alert(feature) for feature in data["features"]]
    return "\n---\n".join(alerts)

@mcp.tool()
async def get_forecast(latitude: float, longitude: float) -> str:
    """Get weather forecast for a location.

    Args:
        latitude: Latitude of the location
        longitude: Longitude of the location
    """
    # First get the forecast grid endpoint
    points_url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
    points_data = await make_nws_request(points_url)

    if not points_data:
        return "Unable to fetch forecast data for this location."

    # Get the forecast URL from the points response
    forecast_url = points_data["properties"]["forecast"]
    forecast_data = await make_nws_request(forecast_url)

    if not forecast_data:
        return "Unable to fetch detailed forecast."

    # Format the periods into a readable forecast
    periods = forecast_data["properties"]["periods"]
    forecasts = []
    for period in periods[:5]:  # Only show next 5 periods
        forecast = f"""
{period['name']}:
Temperature: {period['temperature']}°{period['temperatureUnit']}
Wind: {period['windSpeed']} {period['windDirection']}
Forecast: {period['detailedForecast']}
"""
        forecasts.append(forecast)

    return "\n---\n".join(forecasts)

@mcp.tool()
async def search_excel_data(filename: str = "thongtincanhan.xlsx", search_term: str = "", directory: str = "D:/XuLy_Data/MCP/web_app/excel_files") -> str:
    """
    Tìm kiếm thông tin trong file Excel dựa trên từ khóa.
    
    Args:
        filename: Tên file Excel (không cần đường dẫn đầy đủ)
        search_term: Từ khóa cần tìm kiếm (để trống để xem tất cả dữ liệu)
        directory: Thư mục chứa các file Excel (mặc định: D:/XuLy_Data/MCP/web_app/excel_files)
    """
    # Tạo thư mục nếu chưa tồn tại
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        return f"Đã tạo thư mục {directory}. Vui lòng thêm file Excel vào thư mục này."
    
    # Xây dựng đường dẫn đầy đủ
    filepath = os.path.join(directory, filename)
    
    if not os.path.exists(filepath):
        # Liệt kê các file Excel có sẵn trong thư mục
        excel_files = [f for f in os.listdir(directory) if f.endswith(('.xlsx', '.xls'))]
        if excel_files:
            file_list = "\n".join(excel_files)
            return f"Không tìm thấy file '{filename}' tại thư mục: {directory}\n\nCác file Excel có sẵn:\n{file_list}"
        else:
            return f"Không tìm thấy file '{filename}' và không có file Excel nào trong thư mục: {directory}"

    try:
        wb = openpyxl.load_workbook(filepath)
        sheet = wb.active
        
        # Lấy tiêu đề cột (hàng đầu tiên)
        headers = []
        for cell in sheet[1]:
            headers.append(str(cell.value) if cell.value is not None else "")
        
        results = []
        found = False
        
        # Bắt đầu từ hàng thứ 2 (sau tiêu đề)
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), 2):
            row_data = {}
            row_text = ""
            
            # Kết hợp tiêu đề và giá trị
            for i, cell_value in enumerate(row):
                if i < len(headers):
                    header = headers[i]
                    value = str(cell_value) if cell_value is not None else ""
                    row_data[header] = value
                    row_text += f"{value} "
            
            # Nếu không có từ khóa hoặc tìm thấy từ khóa trong hàng
            if not search_term or search_term.lower() in row_text.lower():
                found = True
                # Tạo chuỗi kết quả có định dạng
                result_str = f"Dòng {row_idx}:\n"
                for header, value in row_data.items():
                    if header:  # Chỉ hiển thị các cột có tiêu đề
                        result_str += f"  {header}: {value}\n"
                results.append(result_str)
        
        if not found:
            return f"Không tìm thấy dữ liệu nào chứa từ khóa '{search_term}' trong file {filename}."
            
        if not results:
            return f"File {filename} không có dữ liệu hoặc chỉ có tiêu đề."
            
        return f"Kết quả tìm kiếm trong file {filename}:\n\n" + "\n".join(results)

    except Exception as e:
        return f"Lỗi khi đọc và tìm kiếm trong file Excel {filename}: {str(e)}"

@mcp.tool()
async def list_excel_files(directory: str = "D:/XuLy_Data/MCP/web_app/excel_files") -> str:
    """
    Liệt kê tất cả các file Excel trong thư mục chỉ định.
    
    Args:
        directory: Thư mục chứa các file Excel (mặc định: D:/XuLy_Data/MCP/web_app/excel_files)
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        return f"Đã tạo thư mục {directory}. Hiện chưa có file Excel nào."
    
    excel_files = [f for f in os.listdir(directory) if f.endswith(('.xlsx', '.xls'))]
    
    if not excel_files:
        return f"Không có file Excel nào trong thư mục: {directory}"
    
    return f"Danh sách file Excel trong thư mục {directory}:\n\n" + "\n".join(excel_files)

if __name__ == "__main__":
    # Chạy server FastMCP
    mcp.run(transport='stdio')
