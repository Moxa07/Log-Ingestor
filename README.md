# Log-Ingestor


from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)
DB_NAME = 'logs.db'

def create_table():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS logs
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        level TEXT, message TEXT, resourceId TEXT,
                        timestamp TEXT, traceId TEXT, spanId TEXT,
                        commit TEXT, parentResourceId TEXT)''')

@app.route('/ingest', methods=['POST'])
def ingest_log():
    try:
        log_data = request.get_json()
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO logs (level, message, resourceId, timestamp, traceId, spanId, commit, parentResourceId)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                           (log_data['level'], log_data['message'], log_data['resourceId'],
                            log_data['timestamp'], log_data['traceId'], log_data['spanId'],
                            log_data['commit'], log_data['metadata']['parentResourceId']))
            conn.commit()
        return jsonify({'message': 'Log ingested successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    create_table()
    app.run(port=3000)
