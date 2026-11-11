from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
import paho.mqtt.client as mqtt
import pymysql
import uuid
from functools import wraps

# Biến lưu trữ token tạm thời trên RAM (token -> user_id)
active_tokens = {}

# Decorator kiểm tra xác thực Token
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        token = None
        
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            
        if not token or token not in active_tokens:
            return jsonify({"status": "error", "message": "Unauthorized: Token không hợp lệ hoặc chưa đăng nhập!"}), 401
            
        return f(*args, **kwargs)
    return decorated

app = Flask(__name__)
CORS(app)  # Cho phép giao diện HTML gọi API

# --- CẤU HÌNH KẾT NỐI MYSQL (Database tên là 'iot') ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""  # Điền mật khẩu MySQL của bạn nếu có
DB_NAME = "iot"


def get_db_connection():
  return pymysql.connect(
      host=DB_HOST,
      user=DB_USER,
      password=DB_PASSWORD,
      database=DB_NAME,
      cursorclass=pymysql.cursors.DictCursor,
  )


# --- CẤU HÌNH MQTT BROKER ---
MQTT_BROKER = "10.32.41.46"  # IP Mosquitto Broker của bạn
MQTT_PORT = 1883
TOPIC_SENSOR = "data_sensors"
TOPIC_CONTROL = "device_control"
TOPIC_RESPONSE = "device_response"

# Biến lưu trữ trạng thái cảm biến mới nhất để trả về dashboard nhanh
latest_sensor_data = {
    "temperature": "-- °C",
    "humidity": "-- %",
    "light": "-- Lux",
}


def on_connect(client, userdata, flags, rc):
  print(f"Connected to MQTT Broker with result code {rc}")
  client.subscribe(TOPIC_SENSOR)
  client.subscribe(TOPIC_RESPONSE)


def on_message(client, userdata, msg):
  global latest_sensor_data
  payload = msg.payload.decode("utf-8")
  topic = msg.topic
  print(f"Received MQTT [Topic: {topic}]: {payload}")

  connection = get_db_connection()
  try:
    with connection.cursor() as cursor:
      if topic == TOPIC_SENSOR:
        try:
          parts = payload.split("|")
          # Chỉ lấy 3 thông số cơ bản đầu tiên: Temp, Humi, Light
          temp_str = parts[0].split(":")[1].replace("C", "").strip()
          humi_str = parts[1].split(":")[1].replace("%", "").strip()
          light_str = parts[2].split(":")[1].strip()

          t_val = float(temp_str)
          h_val = float(humi_str)
          l_val = float(light_str)

          # Cập nhật biến tạm cho dashboard chính
          latest_sensor_data["temperature"] = f"{t_val} °C"
          latest_sensor_data["humidity"] = f"{h_val} %"
          latest_sensor_data["light"] = f"{l_val} Lux"

          now = datetime.now()
          # 1. Thêm dữ liệu cảm biến mới vào bảng data_sensors
          sql_insert = """INSERT INTO data_sensors (sensor_id, value, recorded_at, created_at) VALUES (%s, %s, %s, %s)"""
          cursor.execute(sql_insert, (1, t_val, now, now))
          cursor.execute(sql_insert, (2, h_val, now, now))
          cursor.execute(sql_insert, (3, l_val, now, now))
          connection.commit()

          # 2. Dọn dẹp dữ liệu cũ (giữ lại 500 bản ghi mới nhất)
          try:
              sql_cleanup = """
                  DELETE FROM data_sensors 
                  WHERE data_id NOT IN (
                      SELECT data_id FROM (
                          SELECT data_id FROM data_sensors 
                          ORDER BY data_id DESC 
                          LIMIT 500
                      ) AS temp_table
                  )
              """
              cursor.execute(sql_cleanup)
              connection.commit()
          except Exception as cleanup_err:
              connection.rollback()
              print(f"Bỏ qua dọn dẹp do xung đột khóa tạm thời: {cleanup_err}")
              
        except Exception as e:
          print(f"Lỗi parse payload cảm biến: {e}")

      elif topic == TOPIC_RESPONSE:
        payload_upper = payload.upper()
        status_val = "OFF" if "OFF" in payload_upper else "ON"
        
        # Xác định device_id dựa vào tên LED phản hồi
        dev_id = 1
        if "LED2" in payload_upper:
            dev_id = 2
        elif "LED3" in payload_upper:
            dev_id = 3
        elif "LED4" in payload_upper:
            dev_id = 4
        elif "LED5" in payload_upper:
            dev_id = 5

        now = datetime.now()
        # 1. Cập nhật bảng actions
        sql_action = """UPDATE actions SET status = %s, time_response = %s WHERE status = 'LOADING' ORDER BY action_id DESC LIMIT 1"""
        cursor.execute(sql_action, (status_val, now))

        # 2. Cập nhật trạng thái mới nhất cho bảng devices
        sql_device = "UPDATE devices SET status = %s WHERE device_id = %s"
        cursor.execute(sql_device, (status_val.lower(), dev_id))

    connection.commit()
  finally:
    connection.close()


mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
try:
  mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
  mqtt_client.loop_start()
except Exception as e:
  print(f"Không thể kết nối MQTT Broker: {e}")


# --- API CHO GIAO DIỆN WEB & POSTMAN ---


# 1. API lấy thông tin cảm biến mới nhất cho Dashboard
@app.route("/api/sensors", methods=["GET"])
def api_get_sensors():
  return jsonify(latest_sensor_data)


@app.route("/api/control", methods=["POST"])
@token_required
def api_control_device():
  data = request.json
  command = data.get("command")  # Ví dụ: "LED1 ON", "LED3 ON",...

  if not command:
    return jsonify({"status": "error", "message": "Thiếu thông tin lệnh (command)"}), 400

  cmd_upper = command.upper()
  
  # Gửi lệnh xuống phần cứng ESP8266 qua MQTT (Tất cả các đèn đều nhận được lệnh để sáng/tắt)
  mqtt_client.publish(TOPIC_CONTROL, command)

  # CHỈ LƯU VÀO BẢNG ACTIONS NẾU LÀ LED 1 HOẶC LED 2. CÒN LED 3, 4, 5 THÌ BỎ QUA KHÔNG GHI GÌ CẢ.
  if "LED1" in cmd_upper or "LED2" in cmd_upper or "ALL" in cmd_upper:
      device_id = 2 if "LED2" in cmd_upper else 1
      action_val = "ON" if "ON" in cmd_upper else "OFF"
      status_val = "LOADING" 

      connection = get_db_connection()
      try:
        with connection.cursor() as cursor:
          sql = """INSERT INTO actions (device_id, user_id, action, status, time) 
                   VALUES (%s, %s, %s, %s, %s)"""
          cursor.execute(sql, (device_id, 1, action_val, status_val, datetime.now()))
        connection.commit()
      finally:
        connection.close()

  return jsonify({"status": "success", "message": f"Đã gửi lệnh {command} thành công!"})


# 3. API lấy lịch sử thao tác cho trang action_history.html
@app.route("/api/history", methods=["GET"])
@token_required
def api_get_history():
  connection = get_db_connection()
  try:
    with connection.cursor() as cursor:
      # JOIN bảng actions với devices để lấy tên thiết bị hiển thị trực quan lên web
      sql = """
                SELECT a.action_id as id, d.device_name, a.action, a.status, a.time, a.time_response 
                FROM actions a
                LEFT JOIN devices d ON a.device_id = d.device_id
                ORDER BY a.action_id DESC LIMIT 50
            """
      cursor.execute(sql)
      result = cursor.fetchall()
      return jsonify(result)
  finally:
    connection.close()

# API lấy lịch sử dữ liệu cảm biến cho trang data_sensors.html
@app.route('/api/sensors-history', methods=['GET'])
@token_required
def api_get_sensors_history():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT ds.data_id as id, 'ESP8266' as device_name, 
                       s.sensor_name as sensor_name, 
                       CONCAT(ds.value, ' ', IFNULL(s.unit, '')) as value, 
                       ds.recorded_at as time
                FROM data_sensors ds
                LEFT JOIN sensors s ON ds.sensor_id = s.sensor_id
                ORDER BY ds.data_id DESC LIMIT 100
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            return jsonify(result)
    finally:
        connection.close()

@app.route("/api/devices", methods=["GET"])
@token_required
def api_get_devices():
  connection = get_db_connection()
  try:
    with connection.cursor() as cursor:
      cursor.execute("SELECT device_id, device_name, status FROM devices")
      devices = cursor.fetchall()
      return jsonify(devices)
  finally:
    connection.close()

# API Đăng nhập để cấp Token
@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.json
    username = data.get("username")
    password = data.get("password")
    
    if not username or not password:
        return jsonify({"status": "error", "message": "Vui lòng nhập tài khoản và mật khẩu!"}), 400
        
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Kiểm tra tài khoản trong bảng users
            sql = "SELECT user_id, username FROM users WHERE username = %s AND password = %s"
            cursor.execute(sql, (username, password))
            user = cursor.fetchone()
            
            if user:
                # Tạo Token ngẫu nhiên
                token = str(uuid.uuid4())
                active_tokens[token] = user['user_id']
                return jsonify({
                    "status": "success",
                    "message": "Đăng nhập thành công!",
                    "token": token,
                    "user": user
                })
            else:
                return jsonify({"status": "error", "message": "Sai tài khoản hoặc mật khẩu!"}), 401
    finally:
        connection.close()

# API nhận giá trị random từ dashboard2 để lưu vào bảng data_sensors
@app.route("/api/save-random", methods=["POST"])
@token_required
def api_save_random():
    data = request.json
    r1 = data.get("val1")
    r2 = data.get("val2")
    r3 = data.get("val3")
    
    if r1 is None or r2 is None or r3 is None:
        return jsonify({"status": "error", "message": "Thiếu dữ liệu"}), 400

    now = datetime.now()
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql_insert = """INSERT INTO data_sensors (sensor_id, value, recorded_at, created_at) VALUES (%s, %s, %s, %s)"""
            cursor.execute(sql_insert, (4, r1, now, now))  # Lưu Random 0-11
            cursor.execute(sql_insert, (5, r2, now, now))  # Lưu Random 0-101
            cursor.execute(sql_insert, (6, r3, now, now))  # Lưu Random 0-1001
            connection.commit()
        return jsonify({"status": "success", "message": "Đã lưu giá trị random thành công!"})
    except Exception as e:
        print(f"Lỗi lưu random: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        connection.close()

# --- Bổ sung API Đăng xuất để xóa token trên server ---
@app.route("/api/logout", methods=["POST"])
@token_required
def api_logout():
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        if token in active_tokens:
            del active_tokens[token]
    return jsonify({"status": "success", "message": "Đăng xuất thành công!"})

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000, debug=True)