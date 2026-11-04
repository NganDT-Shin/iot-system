from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
import paho.mqtt.client as mqtt
import pymysql

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
MQTT_BROKER = "192.168.247.7"  # IP Mosquitto Broker của bạn
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
        # Ví dụ chuỗi payload từ ESP8266: "Temp: 28.5C | Humi: 65% | Light: 450 | LED1: OFF | LED2: OFF"
        # Tách chuỗi cơ bản để lấy giá trị (hoặc bạn có thể tối ưu format gửi từ ESP8266 dạng JSON cho dễ parse)
        try:
          parts = payload.split("|")
          temp_str = parts[0].split(":")[1].replace("C", "").strip()
          humi_str = parts[1].split(":")[1].replace("%", "").strip()
          light_str = parts[2].split(":")[1].strip()

          t_val = float(temp_str)
          h_val = float(humi_str)
          l_val = float(light_str)

          # Cập nhật biến tạm cho dashboard
          latest_sensor_data["temperature"] = f"{t_val} °C"
          latest_sensor_data["humidity"] = f"{h_val} %"
          latest_sensor_data["light"] = f"{l_val} Lux"

          now = datetime.now()
          # Lưu vào bảng data_sensors ứng với các sensor_id (1=Nhiệt độ, 2=Độ ẩm, 3=Ánh sáng)
          sql = """INSERT INTO data_sensors (sensor_id, value, recorded_at, created_at) VALUES (%s, %s, %s, %s)"""
          cursor.execute(sql, (1, t_val, now, now))
          cursor.execute(sql, (2, h_val, now, now))
          cursor.execute(sql, (3, l_val, now, now))

          # TỰ ĐỘNG XÓA DỮ LIỆU CŨ: Giữ lại đúng 500 bản ghi mới nhất dựa trên khóa chính data_id
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
          print("Đã xóa dữ liệu cũ thành công.")
        except Exception as e:
          print(f"Lỗi parse payload cảm biến: {e}")

      elif topic == TOPIC_RESPONSE:
        payload_upper = payload.upper()
        status_val = "off" if "OFF" in payload_upper else "on"
        
        # Xác định device_id dựa vào phản hồi (VD: "LED1" -> id=1)
        dev_id = 2 if "LED2" in payload_upper else 1

        now = datetime.now()
        # 1. Cập nhật bảng actions
        sql_action = """UPDATE actions SET status = %s, time_response = %s WHERE status = 'loading' ORDER BY action_id DESC LIMIT 1"""
        cursor.execute(sql_action, (status_val, now))

        # 2. Cập nhật trạng thái mới nhất cho bảng devices
        sql_device = "UPDATE devices SET status = %s WHERE device_id = %s"
        cursor.execute(sql_device, (status_val, dev_id))

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


# 2. API điều khiển thiết bị từ Giao diện / Postman xuống ESP8266
@app.route("/api/control", methods=["POST"])
def api_control_device():
  data = request.json
  command = data.get("command")  # Ví dụ: "LED1 ON" hoặc "LED2 OFF"

  if not command:
    return (
        jsonify({"status": "error", "message": "Thiếu thông tin lệnh (command)"}),
        400,
    )

  # Xác định device_id dựa vào tên thiết bị trong lệnh (LED1 -> id=1, LED2 -> id=2)
  device_id = 2 if "LED2" in command.upper() else 1

  # Gửi lệnh xuống ESP8266 thông qua MQTT Broker
  mqtt_client.publish(TOPIC_CONTROL, command)

  # Lưu lịch sử thao tác vào bảng actions với đúng device_id và chuỗi command thực tế
  connection = get_db_connection()
  try:
    with connection.cursor() as cursor:
      sql = """INSERT INTO actions (device_id, user_id, action, status, time) 
               VALUES (%s, %s, %s, %s, %s)"""
      cursor.execute(sql, (device_id, 1, command, "loading", datetime.now()))
    connection.commit()
  finally:
    connection.close()

  return jsonify(
      {"status": "success", "message": f"Đã gửi lệnh {command} thành công!"}
  )


# 3. API lấy lịch sử thao tác cho trang action_history.html
@app.route("/api/history", methods=["GET"])
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
def api_get_devices():
  connection = get_db_connection()
  try:
    with connection.cursor() as cursor:
      cursor.execute("SELECT device_id, device_name, status FROM devices")
      devices = cursor.fetchall()
      return jsonify(devices)
  finally:
    connection.close()


if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000, debug=True)