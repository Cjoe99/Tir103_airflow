from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

# API 端點來執行 Python 腳本
@app.route('/run-script', methods=['POST'])
def run_script():
    try:
        # 接收參數（例如，腳本路徑或參數）
        data = request.get_json()
        script_path = data.get('script_path', 'path_to_script.py')  # 替換為預設的腳本路徑
        args = data.get('args', [])  # 可選參數

        # 使用 subprocess 在宿主機上執行 Python 腳本
        result = subprocess.run(['python3', script_path] + args, capture_output=True, text=True)
        
        # 返回執行結果
        return jsonify({
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
